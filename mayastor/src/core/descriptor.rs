use std::{
    convert::TryFrom,
    fmt::{Debug, Error, Formatter},
    os::raw::c_void,
};

use futures::channel::oneshot;

use spdk_rs::{
    libspdk::{
        bdev_lock_lba_range,
        bdev_unlock_lba_range,
        spdk_bdev_desc,
        spdk_bdev_get_io_channel,
    },
    BdevModule,
};

use crate::{
    bdev::nexus::NEXUS_MODULE_NAME,
    core::{
        channel::IoChannel,
        Bdev,
        BdevHandle,
        CoreError,
        Mthread,
        UntypedBdev,
    },
};

/// NewType around a descriptor, multiple descriptor to the same bdev is
/// allowed. A bdev can be claimed for exclusive write access. Any existing
/// descriptors that are open before the bdev has been claimed will remain as
/// is. Typically, the target, exporting the bdev will claim the device. In the
/// case of the nexus, we do not claim the children for exclusive access to
/// allow for the rebuild to happen across multiple cores.
pub struct Descriptor(spdk_rs::BdevDesc<()>);

impl Descriptor {
    /// TODO
    pub(crate) fn new(d: spdk_rs::BdevDesc<()>) -> Self {
        Self(d)
    }

    /// returns the underling ptr
    pub fn as_ptr(&self) -> *mut spdk_bdev_desc {
        self.0.legacy_as_ptr()
    }

    /// Get a channel to the underlying bdev
    pub fn get_channel(&self) -> Option<IoChannel> {
        let ch = unsafe { spdk_bdev_get_io_channel(self.0.legacy_as_ptr()) };
        if ch.is_null() {
            error!(
                "failed to get IO channel for {} probably low on memory!",
                self.get_bdev().name(),
            );
            None
        } else {
            Some(IoChannel::from(ch))
        }
    }

    /// claim the bdev for exclusive access, when the descriptor is in read-only
    /// the descriptor will implicitly be upgraded to read/write.
    ///
    /// Conversely, Preexisting writers will not be downgraded.
    pub fn claim(&self) -> bool {
        match BdevModule::find_by_name(NEXUS_MODULE_NAME) {
            Ok(m) => m.claim_bdev(&self.0.bdev(), &self.0).is_ok(),
            Err(err) => {
                error!("{}", err);
                false
            }
        }
    }

    /// unclaim a bdev previously claimed by NEXUS_MODULE
    pub(crate) fn unclaim(&self) {
        match BdevModule::find_by_name(NEXUS_MODULE_NAME) {
            Ok(m) => {
                if let Err(err) = m.release_bdev(&self.0.bdev()) {
                    error!("{}", err)
                }
            }
            Err(err) => {
                error!("{}", err);
            }
        }
    }

    /// release a previously claimed bdev
    pub fn release(&self) {
        self.0.bdev().release_claim();
    }

    /// Return the bdev associated with this descriptor, a descriptor cannot
    /// exist without a bdev
    pub fn get_bdev(&self) -> UntypedBdev {
        Bdev::new(self.0.bdev())
    }

    /// consumes the descriptor and returns a handle
    pub fn into_handle(self) -> Result<BdevHandle, CoreError> {
        BdevHandle::try_from(self)
    }

    /// Gain exclusive access over a block range.
    /// The same context must be used when calling unlock.
    pub async fn lock_lba_range(
        &self,
        ctx: &mut RangeContext,
        ch: &IoChannel,
    ) -> Result<(), nix::errno::Errno> {
        let (s, r) = oneshot::channel::<i32>();
        ctx.sender = Box::into_raw(Box::new(s));

        unsafe {
            let rc = bdev_lock_lba_range(
                self.as_ptr(),
                ch.as_ptr(),
                ctx.offset,
                ctx.len,
                Some(spdk_range_cb),
                ctx as *const _ as *mut c_void,
            );
            if rc != 0 {
                return Err(nix::errno::from_i32(rc));
            }
        }

        // Wait for the lock to complete
        let rc = r.await.unwrap();
        if rc != 0 {
            return Err(nix::errno::from_i32(rc));
        }

        Ok(())
    }

    /// Release exclusive access over a block range.
    /// The context must match the one used by the call to lock.
    pub async fn unlock_lba_range(
        &self,
        ctx: &mut RangeContext,
        ch: &IoChannel,
    ) -> Result<(), nix::errno::Errno> {
        let (s, r) = oneshot::channel::<i32>();
        ctx.sender = Box::into_raw(Box::new(s));

        unsafe {
            let rc = bdev_unlock_lba_range(
                self.as_ptr(),
                ch.as_ptr(),
                ctx.offset,
                ctx.len,
                Some(spdk_range_cb),
                ctx as *const _ as *mut c_void,
            );
            if rc != 0 {
                return Err(nix::errno::from_i32(rc));
            }
        }

        // Wait for the unlock to complete
        let rc = r.await.unwrap();
        if rc != 0 {
            return Err(nix::errno::from_i32(rc));
        }

        Ok(())
    }
}

/// when we get removed we might be asked to close ourselves
/// however, this request might come from a different thread as
/// targets (for example) are running on their own thread.
impl Drop for Descriptor {
    fn drop(&mut self) {
        trace!("[D] {:?}", self);
        if Mthread::current().unwrap() == Mthread::get_init() {
            self.0.close()
        } else {
            Mthread::get_init().msg(self.0.clone(), |mut d| d.close());
        }
    }
}

impl Debug for Descriptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "Descriptor {:p} for bdev: {}",
            self.as_ptr(),
            self.0.bdev().name()
        )
    }
}

extern "C" fn spdk_range_cb(
    ctx: *mut ::std::os::raw::c_void,
    status: ::std::os::raw::c_int,
) {
    unsafe {
        let ctx = ctx as *mut RangeContext;
        let s = Box::from_raw((*ctx).sender as *mut oneshot::Sender<i32>);

        // Send a notification that the operation has completed
        if let Err(e) = s.send(status) {
            panic!("Failed to send SPDK completion with error {}.", e);
        }
    }
}

/// Store the context for calls to lock_lba_range and unlock_lba_range.
/// Corresponding lock/unlock calls require the same context to be used.
pub struct RangeContext {
    pub offset: u64,
    pub len: u64,
    sender: *mut oneshot::Sender<i32>,
}

impl RangeContext {
    /// Create a new RangeContext
    pub fn new(offset: u64, len: u64) -> RangeContext {
        RangeContext {
            offset,
            len,
            sender: std::ptr::null_mut(),
        }
    }
}
