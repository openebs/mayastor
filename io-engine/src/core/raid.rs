use std::{
    fmt::{Debug, Formatter},
    ptr::{self, NonNull},
    time::Duration,
};

use futures::channel::oneshot;
use nix::errno::Errno;

use crate::{
    bdev_api::BdevError,
    core::UntypedBdev,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult, IntoCString},
};

use spdk_rs::{
    libspdk,
    libspdk::{
        raid_bdev, raid_bdev_add_base_bdev, raid_bdev_create, raid_bdev_delete,
        raid_bdev_find_by_name, spdk_uuid, RAID_BDEV_STATE_CONFIGURING, RAID_BDEV_STATE_OFFLINE,
        RAID_BDEV_STATE_ONLINE,
    },
    PollerBuilder, Uuid as SpdkUuid,
};

pub struct RaidBdev {
    inner: NonNull<raid_bdev>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaidState {
    /// RAID is waiting for more devices to be added
    WaitingForDevices,
    /// RAID has sufficient devices and is transitioning to online (starting up)
    StartingUp,
    /// RAID is fully online and operational
    Online,
    /// RAID is offline/failed
    Offline,
    /// RAID state is unknown or invalid
    Unknown,
}

impl AsRef<str> for RaidState {
    fn as_ref(&self) -> &str {
        match self {
            RaidState::WaitingForDevices => "waiting for devices",
            RaidState::StartingUp => "starting up",
            RaidState::Online => "online",
            RaidState::Offline => "offline",
            RaidState::Unknown => "unknown",
        }
    }
}

impl RaidState {
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl std::fmt::Display for RaidState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RaidDeviceCounts {
    /// number of base bdevs discovered and configured
    pub discovered: u8,
    /// number of base bdevs we expect to be operational (can be less for degraded arrays - depends on raid level)
    pub operational: u8,
    /// total number of base bdevs comprising the RAID array
    pub total: u8,
}

use snafu::Snafu;

/// Errors that can occur while waiting for RAID to reach online state
#[derive(Debug, Snafu, Clone)]
pub enum RaidWaitError {
    /// RAID waiting timed out after the specified duration
    #[snafu(display("RAID waiting timed out after {:?}", duration))]
    Timeout { duration: Duration },

    /// RAID transitioned to a state that may require action
    #[snafu(display("RAID transitioned to failed state: {}", state))]
    FailedState { state: RaidState },

    /// Wait operation was cancelled
    #[snafu(display("Wait operation was cancelled"))]
    Cancelled,
}

impl From<RaidWaitError> for BdevError {
    fn from(err: RaidWaitError) -> Self {
        match err {
            RaidWaitError::Timeout { duration } => BdevError::CreateBdevFailedStr {
                error: format!("RAID wait timeout after {duration:?}"),
                name: "raid".to_string(),
            },
            RaidWaitError::FailedState { state } => BdevError::CreateBdevFailedStr {
                error: format!("RAID is not online, it is in {state} state"),
                name: "raid".to_string(),
            },
            RaidWaitError::Cancelled => BdevError::CreateBdevFailedStr {
                error: "RAID wait cancelled".to_string(),
                name: "raid".to_string(),
            },
        }
    }
}

impl Debug for RaidBdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RaidBdev {{ num_base_bdevs: {}, member_names: {:?} }}",
            self.num_bdevs(),
            self.member_disk_names()
        )
    }
}

impl UntypedBdev {
    /// Try to get this bdev as a RAID bdev.
    pub fn as_raid_bdev(&self) -> Option<RaidBdev> {
        if self.driver() != "raid" {
            return None;
        }

        let name_cstring = self.name().into_cstring();
        let raid_bdev_ptr = unsafe { raid_bdev_find_by_name(name_cstring.as_ptr()) };

        if raid_bdev_ptr.is_null() {
            return None;
        }

        Some(RaidBdev::from_ptr(raid_bdev_ptr))
    }
}

impl RaidBdev {
    /// Create a RaidBdev wrapper from a raw pointer
    pub fn from_ptr(ptr: *mut raid_bdev) -> Self {
        let inner = NonNull::new(ptr).unwrap();
        Self { inner }
    }

    // Create a RAID bdev using SPDK API
    pub fn create(
        name: &str,
        uuid: Option<uuid::Uuid>,
        strip_size_kb: u32,
        num_devices: u8,
        level: i32,
    ) -> Result<Self, BdevError> {
        let cname = name.to_string().into_cstring();

        let uuid_ptr = uuid.as_ref().map_or(ptr::null(), |uuid| {
            let spdk_uuid = SpdkUuid::from(*uuid).into_raw();
            &spdk_uuid as *const spdk_uuid
        });

        // RAID0 and RAID5F require a strip size. Others might fail if we set it.
        let strip_size = if level == libspdk::RAID0 || level == libspdk::RAID5F {
            strip_size_kb * 1024
        } else {
            0
        };

        let mut raid_bdev_ptr: *mut raid_bdev = ptr::null_mut();
        let errno = unsafe {
            raid_bdev_create(
                cname.as_ptr(),
                strip_size,
                num_devices,
                level,
                false,
                uuid_ptr,
                &mut raid_bdev_ptr,
            )
        };

        if errno != 0 {
            return Err(BdevError::CreateBdevFailed {
                source: Errno::from_raw(errno.abs()),
                name: name.to_string(),
            });
        }

        if raid_bdev_ptr.is_null() {
            return Err(BdevError::CreateBdevFailedStr {
                error: "RAID bdev pointer is null after creation".to_string(),
                name: name.to_string(),
            });
        }

        Ok(Self::from_ptr(raid_bdev_ptr))
    }

    // Find a RAID bdev by name
    pub fn find_by_name(name: &str) -> Option<Self> {
        let name = name.to_string().into_cstring();
        let bdev = unsafe { raid_bdev_find_by_name(name.as_ptr()) };
        if !bdev.is_null() {
            Some(Self::from_ptr(bdev))
        } else {
            None
        }
    }

    // Add a base bdev to a RAID bdev
    pub async fn add_base_bdev(&mut self, device_name: &str) -> Result<(), BdevError> {
        let device_name_cstring = device_name.to_string().into_cstring();
        let (s, r) = oneshot::channel::<ErrnoResult<()>>();
        let errno = unsafe {
            raid_bdev_add_base_bdev(
                self.inner.as_ptr(),
                device_name_cstring.as_ptr(),
                Some(done_errno_cb),
                cb_arg(s),
            )
        };
        if errno != 0 {
            return Err(BdevError::CreateBdevFailed {
                source: Errno::from_raw(errno.abs()),
                name: device_name.to_string(),
            });
        }
        r.await
            .map_err(|_| BdevError::CreateBdevFailedStr {
                error: "Failed to receive callback response".to_string(),
                name: device_name.to_string(),
            })?
            .map_err(|errno| BdevError::CreateBdevFailed {
                source: errno,
                name: device_name.to_string(),
            })?;
        Ok(())
    }

    /// Get a reference to the underlying SPDK structure
    fn as_inner_ref(&self) -> &raid_bdev {
        unsafe { self.inner.as_ref() }
    }

    /// Get the name of the RAID bdev
    pub fn name(&self) -> &str {
        unsafe {
            let raid_bdev = self.inner.as_ref();
            if raid_bdev.bdev.name.is_null() {
                ""
            } else {
                std::ffi::CStr::from_ptr(raid_bdev.bdev.name)
                    .to_str()
                    .unwrap()
            }
        }
    }

    /// Get the level of the RAID bdev
    pub fn level(&self) -> i32 {
        self.as_inner_ref().level
    }

    pub fn block_len(&self) -> u32 {
        self.as_inner_ref().bdev.blocklen
    }

    pub fn block_cnt(&self) -> u64 {
        self.as_inner_ref().bdev.blockcnt
    }

    /// Get the capacity of the RAID bdev in bytes
    pub fn capacity(&self) -> u64 {
        self.block_cnt() * self.block_len() as u64
    }

    /// Get the comprehensive state of the RAID bdev
    pub fn state(&self) -> RaidState {
        let raid_bdev = self.as_inner_ref();
        let device_counts = self.device_counts();

        match raid_bdev.state {
            RAID_BDEV_STATE_ONLINE => RaidState::Online,
            RAID_BDEV_STATE_OFFLINE => RaidState::Offline,
            RAID_BDEV_STATE_CONFIGURING => {
                // Check if we have enough devices to start transitioning to online
                if device_counts.discovered >= device_counts.operational {
                    RaidState::StartingUp
                } else {
                    RaidState::WaitingForDevices
                }
            }
            _ => RaidState::Unknown,
        }
    }

    /// Get the device counts for this RAID bdev
    pub fn device_counts(&self) -> RaidDeviceCounts {
        let raid_bdev = self.as_inner_ref();
        RaidDeviceCounts {
            discovered: raid_bdev.num_base_bdevs_discovered,
            operational: raid_bdev.num_base_bdevs_operational,
            total: raid_bdev.num_base_bdevs,
        }
    }

    /// Check if RAID is in the starting up phase (has enough devices, transitioning to online)
    pub fn is_starting_up(&self) -> bool {
        matches!(self.state(), RaidState::StartingUp)
    }

    pub fn is_online(&self) -> bool {
        matches!(self.state(), RaidState::Online)
    }

    /// Wait for RAID to reach online state using SPDK's polling mechanism
    ///
    /// This should be called after all base devices have been added via add_base_bdev().
    /// If RAID is still in WaitingForDevices state, it's treated as an error.
    pub async fn wait_for_online(&self, timeout: Option<Duration>) -> Result<(), RaidWaitError> {
        use futures::channel::oneshot;
        use std::time::Instant;

        // Check if already online
        if matches!(self.state(), RaidState::Online) {
            return Ok(());
        }

        let (s, r) = oneshot::channel::<Result<(), RaidWaitError>>();

        let start_time = Instant::now();
        let raid_bdev = RaidBdev::from_ptr(self.inner.as_ptr());
        let mut sender = Some(s);
        let _poller = PollerBuilder::<()>::new()
            .with_name(&format!("raid_wait_{}", self.name()))
            .with_interval(Duration::from_millis(100))
            .with_poll_fn(move |_| {
                // Reconstruct RaidBdev from the raw pointer
                let current_state = raid_bdev.state();

                match current_state {
                    RaidState::Online => {
                        // Success! RAID is online
                        if let Some(sender) = sender.take() {
                            let _ = sender.send(Ok(()));
                        }
                        0 // Stop polling
                    }
                    RaidState::StartingUp => {
                        // Check timeout if specified
                        if let Some(timeout_duration) = timeout {
                            if start_time.elapsed() >= timeout_duration {
                                if let Some(sender) = sender.take() {
                                    let _ = sender.send(Err(RaidWaitError::Timeout {
                                        duration: timeout_duration,
                                    }));
                                }
                                return 0; // Stop polling
                            }
                        }
                        // Continue polling - RAID is transitioning to online
                        1
                    }
                    RaidState::WaitingForDevices | RaidState::Offline | RaidState::Unknown => {
                        // All these are error states if we're waiting after add_base_bdev
                        if let Some(sender) = sender.take() {
                            let _ = sender.send(Err(RaidWaitError::FailedState {
                                state: current_state,
                            }));
                        }
                        0 // Stop polling
                    }
                }
            })
            .build();

        // Wait for the result from the poller
        r.await.map_err(|_| RaidWaitError::Cancelled)?
    }

    /// Delete the RAID bdev.
    pub async fn delete(self) -> Result<(), BdevError> {
        let (s, r) = oneshot::channel::<ErrnoResult<()>>();
        unsafe {
            raid_bdev_delete(self.inner.as_ptr(), Some(done_errno_cb), cb_arg(s));
        }
        r.await
            .map_err(|_| BdevError::DestroyBdevFailedStr {
                error: "Failed to receive callback response".to_string(),
                name: self.name().to_string(),
            })?
            .map_err(|errno| BdevError::DestroyBdevFailed {
                source: errno,
                name: self.name().to_string(),
            })?;
        Ok(())
    }

    /// Get the number of base bdevs in the RAID bdev
    pub fn num_bdevs(&self) -> usize {
        self.as_inner_ref().num_base_bdevs as usize
    }

    /// Get the names of all member disks in this RAID bdev
    pub fn member_disk_names(&self) -> Vec<String> {
        let raid_bdev = self.as_inner_ref();
        let num_base_bdevs = raid_bdev.num_base_bdevs as usize;

        if num_base_bdevs == 0 || raid_bdev.base_bdev_info.is_null() {
            return Vec::new();
        }

        let mut names = Vec::with_capacity(num_base_bdevs);

        unsafe {
            let base_bdev_info =
                std::slice::from_raw_parts(raid_bdev.base_bdev_info, num_base_bdevs);

            for info in base_bdev_info {
                if !info.name.is_null() {
                    let c_str = std::ffi::CStr::from_ptr(info.name);
                    if let Ok(name_str) = c_str.to_str() {
                        names.push(name_str.to_string());
                    }
                }
            }
        }

        names
    }

    /// Get UntypedBdev objects for all available member disks in this RAID bdev.
    /// The returned vector may be shorter than num_base_bdevs if some members
    /// are not yet initialized or configured.
    pub fn member_bdevs(&self) -> Vec<crate::core::UntypedBdev> {
        self.iter_member_bdevs().collect()
    }

    /// Get an iterator over available member bdevs.
    /// This skips uninitialized or invalid member slots.
    pub fn iter_member_bdevs(&self) -> RaidMemberBdevIter<'_> {
        let raid_bdev = self.as_inner_ref();
        let num_base_bdevs = raid_bdev.num_base_bdevs as usize;

        if num_base_bdevs == 0 || raid_bdev.base_bdev_info.is_null() {
            RaidMemberBdevIter {
                base_bdev_info_slice: &[],
                current_index: 0,
            }
        } else {
            let slice =
                unsafe { std::slice::from_raw_parts(raid_bdev.base_bdev_info, num_base_bdevs) };
            RaidMemberBdevIter {
                base_bdev_info_slice: slice,
                current_index: 0,
            }
        }
    }
}

/// Iterator over available RAID member bdevs
pub struct RaidMemberBdevIter<'a> {
    base_bdev_info_slice: &'a [libspdk::raid_base_bdev_info],
    current_index: usize,
}

impl<'a> Iterator for RaidMemberBdevIter<'a> {
    type Item = crate::core::UntypedBdev;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_index < self.base_bdev_info_slice.len() {
            let info = &self.base_bdev_info_slice[self.current_index];
            self.current_index += 1;

            unsafe {
                if !info.desc.is_null() {
                    // Get the bdev from the descriptor - this is the preferred method
                    let bdev_ptr = libspdk::spdk_bdev_desc_get_bdev(info.desc);
                    if !bdev_ptr.is_null() {
                        if let Some(bdev) = crate::core::UntypedBdev::checked_from_ptr(bdev_ptr) {
                            return Some(bdev);
                        }
                    }
                } else if !info.name.is_null() {
                    // Fallback: lookup by name (in case desc is not available)
                    let c_str = std::ffi::CStr::from_ptr(info.name);
                    if let Ok(name_str) = c_str.to_str() {
                        if let Some(bdev) = crate::core::UntypedBdev::lookup_by_name(name_str) {
                            return Some(bdev);
                        }
                    }
                }
            }
            // Continue to next slot if this one was invalid
        }
        None
    }
}
