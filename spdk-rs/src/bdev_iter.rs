use std::marker::PhantomData;

use crate::{
    libspdk::{spdk_bdev, spdk_bdev_first, spdk_bdev_module, spdk_bdev_next},
    Bdev,
    BdevModule,
    BdevOps,
};

impl<BdevData> Bdev<BdevData>
where
    BdevData: BdevOps,
{
    /// TODO
    pub fn iter_all() -> BdevGlobalIter<BdevData> {
        BdevGlobalIter::new()
    }
}

/// TODO
pub struct BdevModuleIter<BdevData>
where
    BdevData: BdevOps,
{
    bdev: *mut spdk_bdev,
    bdev_module: *mut spdk_bdev_module,
    _data: PhantomData<BdevData>,
}

impl<BdevData> BdevModuleIter<BdevData>
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

impl<BdevData> Iterator for BdevModuleIter<BdevData>
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
                    return Some(Self::Item::from_ptr(cur));
                }
            }
        }
        None
    }
}

/// TODO
pub struct BdevGlobalIter<BdevData>
where
    BdevData: BdevOps,
{
    bdev: *mut spdk_bdev,
    _data: PhantomData<BdevData>,
}

impl<BdevData> BdevGlobalIter<BdevData>
where
    BdevData: BdevOps,
{
    fn new() -> Self {
        Self {
            bdev: unsafe { spdk_bdev_first() },
            _data: Default::default(),
        }
    }
}

impl<BdevData> Iterator for BdevGlobalIter<BdevData>
where
    BdevData: BdevOps,
{
    type Item = Bdev<BdevData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bdev.is_null() {
            None
        } else {
            let cur = self.bdev;
            self.bdev = unsafe { spdk_bdev_next(cur) };
            Some(Self::Item::from_ptr(cur))
        }
    }
}
