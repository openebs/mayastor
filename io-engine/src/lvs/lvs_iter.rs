use spdk_rs::libspdk::{
    lvol_store_bdev,
    vbdev_lvol_store_first,
    vbdev_lvol_store_next,
};

use super::{Lvs, LvsBdev};

/// Iterator over available LvsBdevs.
pub struct LvsBdevIter {
    inner: *mut lvol_store_bdev,
}

impl LvsBdevIter {
    /// Returns a new LvsBdev iterator.
    pub(super) fn new() -> Self {
        Self {
            inner: unsafe { vbdev_lvol_store_first() },
        }
    }
}

impl Iterator for LvsBdevIter {
    type Item = LvsBdev;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_null() {
            None
        } else {
            unsafe {
                let current = self.inner;
                self.inner = vbdev_lvol_store_next(current);
                Some(LvsBdev::from_inner_ptr(current))
            }
        }
    }
}

/// iterator over all lvol stores
pub struct LvsIter(LvsBdevIter);

impl LvsIter {
    /// Returns a new Lvs iterator.
    pub(super) fn new() -> Self {
        Self(LvsBdevIter::new())
    }
}

impl Iterator for LvsIter {
    type Item = Lvs;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|l| l.lvs())
    }
}
