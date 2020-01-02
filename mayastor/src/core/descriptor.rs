use crate::core::{channel::IoChannel, Bdev};
use spdk_sys::{
    spdk_bdev_close,
    spdk_bdev_desc,
    spdk_bdev_desc_get_bdev,
    spdk_bdev_get_io_channel,
};

/// new type around a descriptor, only one descriptor is typically available as
/// a bdev is opened only one time. When the last reference to the descriptor is
/// dropped, we implicitly close the bdev.
#[derive(Debug, Clone)]
pub struct Descriptor(pub(crate) *mut spdk_bdev_desc);

impl Drop for Descriptor {
    fn drop(&mut self) {
        trace!("closing bdev: {}", self.get_bdev().unwrap().name());
        unsafe { spdk_bdev_close(self.0) }
    }
}

impl Descriptor {
    pub fn as_ptr(&self) -> *mut spdk_bdev_desc {
        self.0
    }

    pub fn get_channel(&self) -> Option<IoChannel> {
        if self.0.is_null() {
            None
        } else {
            Some(IoChannel(unsafe { spdk_bdev_get_io_channel(self.0) }))
        }
    }

    pub fn get_bdev(&self) -> Option<Bdev> {
        if self.0.is_null() {
            None
        } else {
            Some(Bdev(unsafe { spdk_bdev_desc_get_bdev(self.0) }))
        }
    }
}
