use spdk_rs::libspdk::{lvol_store_bdev, vbdev_lvol_store_first, vbdev_lvol_store_next};

use super::{Lvs, LvsBdev};

/// Iterator over available LvsBdevs.
pub struct LvsBdevIter {
    inner: *mut lvol_store_bdev,
    list_removing: bool,
}

impl LvsBdevIter {
    /// Returns a new LvsBdev iterator.
    pub(super) fn new(list_removing: bool) -> Self {
        Self {
            inner: unsafe { vbdev_lvol_store_first() },
            list_removing,
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
    pub(super) fn new(list_removing: bool) -> Self {
        Self(LvsBdevIter::new(list_removing))
    }
}

impl Iterator for LvsIter {
    type Item = Lvs;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.list_removing {
            self.0.next().map(|l| l.lvs())
        } else {
            self.0.next().and_then(|l| l.lvs_opt())
        }
    }
}
