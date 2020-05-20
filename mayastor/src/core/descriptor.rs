use std::{convert::TryFrom, fmt::Debug};

use serde::export::{fmt::Error, Formatter};

use spdk_sys::{
    bdev_lock_lba_range,
    bdev_unlock_lba_range,
    lock_range_cb,
    spdk_bdev_close,
    spdk_bdev_desc,
    spdk_bdev_desc_get_bdev,
    spdk_bdev_get_io_channel,
    spdk_bdev_module_claim_bdev,
    spdk_bdev_module_release_bdev,
};

use crate::{
    bdev::nexus::nexus_module::NEXUS_MODULE,
    core::{channel::IoChannel, Bdev, BdevHandle, CoreError},
};
use futures::{channel::mpsc, StreamExt};
use std::sync::Arc;

/// NewType around a descriptor, multiple descriptor to the same bdev is
/// allowed. A bdev can me claimed for exclusive write access. Any existing
/// descriptors that are open before the bdev has been claimed will remain as
/// is. Typically, the target, exporting the bdev will claim the device. In the
/// case of the nexus, we do not claim the children for exclusive access to
/// allow for the rebuild to happen across multiple cores.
pub struct Descriptor(*mut spdk_bdev_desc);

impl Descriptor {
    /// returns the underling ptr
    pub fn as_ptr(&self) -> *mut spdk_bdev_desc {
        self.0
    }

    /// Get a channel to the underlying bdev
    pub fn get_channel(&self) -> Option<IoChannel> {
        let ch = unsafe { spdk_bdev_get_io_channel(self.0) };
        if ch.is_null() {
            None
        } else {
            IoChannel::from_null_checked(ch)
        }
    }

    /// claim the bdev for exclusive access, when the descriptor is in read-only
    /// the descriptor will implicitly be upgraded to read/write.
    ///
    /// Conversely, Preexisting writers will not be downgraded.
    pub fn claim(&self) -> bool {
        let err = unsafe {
            spdk_bdev_module_claim_bdev(
                self.get_bdev().as_ptr(),
                self.0,
                NEXUS_MODULE.as_ptr(),
            )
        };

        let name = self.get_bdev().name();
        debug!("claimed bdev {}", name);
        err == 0
    }

    /// release a previously claimed bdev
    pub fn release(&self) {
        unsafe {
            if self.get_bdev().is_claimed() {
                spdk_bdev_module_release_bdev(self.get_bdev().as_ptr())
            }
        }
    }

    /// Return the bdev associated with this descriptor, a descriptor cannot
    /// exist without a bdev
    pub fn get_bdev(&self) -> Bdev {
        let bdev = unsafe { spdk_bdev_desc_get_bdev(self.0) };
        Bdev::from(bdev)
    }

    /// create a Descriptor from a raw spdk_bdev_desc pointer this is the only
    /// way to create a new descriptor
    pub fn from_null_checked(desc: *mut spdk_bdev_desc) -> Option<Descriptor> {
        if desc.is_null() {
            None
        } else {
            Some(Descriptor(desc))
        }
    }

    /// consumes the descriptor and returns a handle
    pub fn into_handle(self) -> Result<BdevHandle, CoreError> {
        BdevHandle::try_from(self)
    }

    /// Gain exclusive access over a block range.
    /// The same context must be used when calling unlock.
    pub async fn lock_lba_range(
        &mut self,
        ctx: &mut RangeContext,
    ) -> Result<(), std::io::Error> {
        unsafe {
            let rc = bdev_lock_lba_range(
                self.as_ptr(),
                ctx.io_channel.as_ptr(),
                ctx.offset,
                ctx.len,
                ctx.cb_fn,
                ctx.sender as *mut _,
            );
            if rc != 0 {
                return Err(std::io::Error::from_raw_os_error(rc));
            }
        }
        let rc = ctx.receiver.next().await.unwrap();
        if rc != 0 {
            return Err(std::io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }

    /// Release exclusive access over a block range.
    /// The context must match the one used by the call to lock.
    pub async fn unlock_lba_range(
        &mut self,
        ctx: &mut RangeContext,
    ) -> Result<(), std::io::Error> {
        unsafe {
            let rc = bdev_unlock_lba_range(
                self.as_ptr(),
                ctx.io_channel.as_ptr(),
                ctx.offset,
                ctx.len,
                ctx.cb_fn,
                ctx.sender as *mut _,
            );
            if rc != 0 {
                return Err(std::io::Error::from_raw_os_error(rc));
            }
        }
        let rc = ctx.receiver.next().await.unwrap();
        if rc != 0 {
            return Err(std::io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        trace!("[D] {:?}", self);
        unsafe {
            spdk_bdev_close(self.0);
        }
    }
}

impl Debug for Descriptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "Descriptor {:p} for bdev: {}",
            self.as_ptr(),
            self.get_bdev().name()
        )
    }
}

extern "C" fn spdk_range_cb(
    ctx: *mut ::std::os::raw::c_void,
    status: ::std::os::raw::c_int,
) {
    unsafe {
        let s = ctx as *mut mpsc::Sender<i32>;
        if let Err(e) = (*s).start_send(status) {
            panic!("Failed to send SPDK completion with error {}.", e);
        }
    }
}

/// Store the context for calls to lock_lba_range and unlock_lba_range.
/// Corresponding lock/unlock calls require the same context to be used.
pub struct RangeContext {
    pub offset: u64,
    pub len: u64,
    io_channel: Arc<IoChannel>,
    cb_fn: lock_range_cb,
    sender: *mut mpsc::Sender<i32>,
    receiver: mpsc::Receiver<i32>,
}

impl RangeContext {
    /// Create a new RangeContext
    pub fn new(offset: u64, len: u64, io_ch: Arc<IoChannel>) -> RangeContext {
        let (s, r) = mpsc::channel::<i32>(0);
        RangeContext {
            offset,
            len,
            io_channel: io_ch,
            cb_fn: Some(spdk_range_cb),
            sender: Box::into_raw(Box::new(s)),
            receiver: r,
        }
    }
}
