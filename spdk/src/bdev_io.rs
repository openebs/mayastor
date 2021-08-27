use crate::{Bdev, BdevOps, IoStatus, IoType};
use spdk_sys::{spdk_bdev_io, spdk_bdev_io_complete};
use std::{marker::PhantomData, ptr::NonNull};

/// Wrapper for SPDK `spdk_bdev_io` data structure.
pub struct BdevIo<BdevContext: BdevOps> {
    inner: NonNull<spdk_bdev_io>,
    _ctx: PhantomData<BdevContext>,
}

impl<BdevContext: BdevOps> BdevIo<BdevContext> {
    /// Makes a new `BdevIo` instance from a raw SPDK structure pointer.
    pub(crate) fn new(bio: *mut spdk_bdev_io) -> Self {
        BdevIo {
            inner: NonNull::new(bio).unwrap(),
            _ctx: Default::default(),
        }
    }

    /// Returns the block device that this I/O belongs to.
    #[inline]
    pub fn bdev(&self) -> Bdev<BdevContext> {
        Bdev::new(unsafe { self.inner.as_ref().bdev })
    }

    /// Determines the type of this I/O.
    #[inline]
    pub fn io_type(&self) -> IoType {
        unsafe { self.inner.as_ref().type_ as u32 }.into()
    }

    /// Marks this I/O as successfull.
    #[inline]
    pub fn ok(&self) {
        self.io_complete(IoStatus::Success);
    }

    /// Marks the IO as failed.
    #[inline]
    pub fn fail(&self) {
        self.io_complete(IoStatus::Failed);
    }

    /// TODO
    #[inline]
    pub fn io_complete(&self, io_status: IoStatus) {
        unsafe {
            spdk_bdev_io_complete(self.inner.as_ptr(), io_status.into());
        }
    }

    /// TODO
    pub fn dbg(&self) -> String {
        format!(
            "BdevIo[bdev <{}> io_type '{:?}']",
            self.bdev().context().dbg(),
            self.io_type()
        )
    }
}
