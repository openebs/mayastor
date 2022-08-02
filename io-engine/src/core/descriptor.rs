use std::{
    convert::TryFrom,
    fmt::{Debug, Error, Formatter},
    ops::Deref,
};

use spdk_rs::{BdevDesc, BdevModule, BdevOps, Thread};

use crate::{
    bdev::nexus::NEXUS_MODULE_NAME,
    core::{Bdev, BdevHandle, CoreError},
};

/// RAII Wrapper for spdk_rs::BdevDesc<T>.
/// When this structure is dropped, the descriptor is closed.
pub struct DescriptorGuard<T: BdevOps>(BdevDesc<T>);

pub type UntypedDescriptorGuard = DescriptorGuard<()>;

impl<T: BdevOps> Deref for DescriptorGuard<T> {
    type Target = BdevDesc<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: BdevOps> DescriptorGuard<T> {
    /// TODO
    pub(crate) fn new(d: BdevDesc<T>) -> Self {
        Self(d)
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
    pub fn unclaim(&self) {
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

    /// Return the bdev associated with this descriptor, a descriptor cannot
    /// exist without a bdev
    pub fn bdev(&self) -> Bdev<T> {
        Bdev::new(self.0.bdev())
    }

    /// Consumes the descriptor and returns a handle.
    pub fn into_handle(self) -> Result<BdevHandle<T>, CoreError> {
        BdevHandle::try_from(self)
    }
}

/// When we get removed we might be asked to close ourselves, however, this
/// request might come from a different thread as targets (for example) are
/// running on their own thread.
impl<T: BdevOps> Drop for DescriptorGuard<T> {
    fn drop(&mut self) {
        if Thread::current().unwrap() == Thread::primary() {
            self.0.close()
        } else {
            Thread::primary().send_msg(self.0.clone(), |mut d| d.close());
        }
    }
}

impl<T: BdevOps> Debug for DescriptorGuard<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "Descriptor {:p} for bdev: {}",
            self.legacy_as_ptr(),
            self.0.bdev().name()
        )
    }
}
