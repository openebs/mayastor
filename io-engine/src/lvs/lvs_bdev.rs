use std::ptr::NonNull;

use spdk_rs::libspdk::lvol_store_bdev;

use crate::core::{Bdev, UntypedBdev};

use super::{Lvs, LvsBdevIter};

/// Structure representing a pool which comprises lvol store and
/// underlying bdev.
///
/// Note about safety: The structure wraps raw C pointers from SPDK.
/// It is safe to use only in synchronous context. If you keep Pool for
/// longer than that then something else can run on reactor_0 in between,
/// which may destroy the pool and invalidate the pointers!
pub struct LvsBdev {
    inner: NonNull<lvol_store_bdev>,
}

impl LvsBdev {
    /// Returns inner SPDK pointer.
    #[inline]
    fn as_inner_ref(&self) -> &lvol_store_bdev {
        unsafe { self.inner.as_ref() }
    }

    /// An easy converter from a raw pointer to Pool object
    pub(super) unsafe fn from_inner_ptr(ptr: *mut lvol_store_bdev) -> LvsBdev {
        LvsBdev {
            inner: NonNull::new(ptr).unwrap(),
        }
    }

    /// Returns Lvs instance for this LVS Bdev.
    #[inline]
    pub(super) fn lvs(&self) -> Lvs {
        Lvs::from_inner_ptr(self.as_inner_ref().lvs)
    }

    /// Get name of the pool.
    pub fn name(&self) -> String {
        self.lvs().name().to_string()
    }

    /// Get base bdev for the pool (in our case AIO or uring bdev).
    pub fn base_bdev(&self) -> UntypedBdev {
        Bdev::checked_from_ptr(self.as_inner_ref().bdev).unwrap()
    }

    /// Iterate Lvs Bdevs.
    pub fn iter() -> LvsBdevIter {
        LvsBdevIter::new()
    }
}
