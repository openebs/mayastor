use crate::{Bdev, BdevModule, BdevOps};

use spdk_sys::{spdk_bdev, spdk_bdev_first, spdk_bdev_module, spdk_bdev_next};
use std::marker::PhantomData;

/// TODO
pub struct BdevIter<BdevData>
where
    BdevData: BdevOps,
{
    bdev: *mut spdk_bdev,
    bdev_module: *mut spdk_bdev_module,
    _data: PhantomData<BdevData>,
}

impl<BdevData> BdevIter<BdevData>
where
    BdevData: BdevOps,
{
    pub(crate) fn new(m: &BdevModule) -> Self {
        Self {
            bdev: unsafe { spdk_bdev_first() },
            bdev_module: m.as_ptr(),
            _data: Default::default(),
        }
    }
}

impl<BdevData> Iterator for BdevIter<BdevData>
where
    BdevData: BdevOps,
{
    type Item = Bdev<BdevData>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.bdev.is_null() {
            let cur = self.bdev;

            unsafe {
                self.bdev = spdk_bdev_next(self.bdev);

                if (*cur).module == self.bdev_module {
                    return Some(Bdev::from_ptr(cur));
                }
            }
        }
        None
    }
}
