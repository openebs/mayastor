use core::fmt;
use std::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
    os::raw::c_void,
    ptr::NonNull,
};

use crate::{nvme::NvmeStatus, Bdev, BdevOps, IoStatus, IoType};

use spdk_sys::{
    spdk_bdev_io,
    spdk_bdev_io_complete,
    spdk_bdev_io_get_buf,
    spdk_io_channel,
};

/// Wrapper for SPDK `spdk_bdev_io` data structure.
pub struct BdevIo<BdevData>
where
    BdevData: BdevOps,
{
    inner: NonNull<spdk_bdev_io>,
    _data: PhantomData<BdevData>,
}

impl<BdevData> BdevIo<BdevData>
where
    BdevData: BdevOps,
{
    /// Returns the block device that this I/O belongs to.
    #[inline]
    pub fn bdev(&self) -> Bdev<BdevData> {
        Bdev::from_ptr(self.as_ref().bdev)
    }

    /// TODO
    #[inline]
    pub fn bdev_checked(&self, prod_name: &str) -> Bdev<BdevData> {
        let b = Bdev::from_ptr(self.as_ref().bdev);
        assert_eq!(b.product_name(), prod_name);
        b
    }

    /// Determines the type of this I/O.
    #[inline]
    pub fn io_type(&self) -> IoType {
        (self.as_ref().type_ as u32).into()
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

    /// Marks the IO as impossible to submit due to a memory constraint.
    #[inline]
    pub fn no_mem(&self) {
        self.io_complete(IoStatus::NoMemory);
    }

    /// TODO
    #[inline]
    pub fn io_complete(&self, io_status: IoStatus) {
        unsafe {
            spdk_bdev_io_complete(self.inner.as_ptr(), io_status.into());
        }
    }

    /// TODO
    /// get a raw pointer to the base of the iov
    #[inline]
    pub fn iovs(&self) -> *mut spdk_sys::iovec {
        unsafe { self.as_ref().u.bdev.iovs }
    }

    /// TODO
    /// number of iovs that are part of this IO
    #[inline]
    pub fn iov_count(&self) -> i32 {
        unsafe { self.as_ref().u.bdev.iovcnt }
    }

    /// TODO
    /// offset where we do the IO on the device
    #[inline]
    pub fn offset(&self) -> u64 {
        unsafe { self.as_ref().u.bdev.offset_blocks }
    }

    /// TODO
    /// num of blocks this IO will read/write/unmap
    #[inline]
    pub fn num_blocks(&self) -> u64 {
        unsafe { self.as_ref().u.bdev.num_blocks }
    }

    /// TODO
    #[inline]
    pub fn status(&self) -> IoStatus {
        self.as_ref().internal.status.into()
    }

    /// TODO
    /// get the block length of this IO
    #[inline]
    #[allow(dead_code)]
    pub fn block_len(&self) -> u64 {
        self.bdev().block_len()
    }

    /// Determines if the IO needs an indirect buffer this can happen for
    /// example when we do a 512 write to a 4k device.
    #[inline]
    #[allow(dead_code)]
    pub fn need_buf(&self) -> bool {
        unsafe {
            let slice = std::slice::from_raw_parts_mut(
                self.iovs(),
                self.iov_count() as usize,
            );

            slice[0].iov_base.is_null()
        }
    }

    /// Returns a mutable reference to the driver context specific for this IO.
    #[inline]
    pub fn driver_ctx_mut<T>(&mut self) -> &mut T {
        unsafe { &mut *(self.as_mut().driver_ctx.as_mut_ptr() as *mut T) }
    }

    /// Returns a reference the driver context specific for this IO.
    #[inline]
    pub fn driver_ctx<T>(&self) -> &T {
        unsafe { &*(self.as_ref().driver_ctx.as_ptr() as *const T) }
    }

    /// TODO
    pub unsafe fn alloc_buffer(
        &mut self,
        cb: extern "C" fn(*mut spdk_io_channel, *mut spdk_bdev_io, bool),
    ) {
        spdk_bdev_io_get_buf(
            self.as_ptr(),
            Some(cb),
            self.num_blocks() * self.block_len(),
        )
    }

    /// TODO
    #[inline]
    pub fn nvme_status(&self) -> NvmeStatus {
        NvmeStatus::from(self.as_ptr())
    }

    /// TODO
    /// NVMe passthru command
    #[inline]
    pub fn nvme_cmd(&self) -> spdk_sys::spdk_nvme_cmd {
        unsafe { self.as_ref().u.nvme_passthru.cmd }
    }

    /// TODO
    /// raw pointer to NVMe passthru data buffer
    #[inline]
    pub fn nvme_buf(&self) -> *mut c_void {
        unsafe { self.as_ref().u.nvme_passthru.buf as *mut _ }
    }

    /// TODO
    /// NVMe passthru number of bytes to transfer
    #[inline]
    pub fn nvme_nbytes(&self) -> u64 {
        unsafe { self.as_ref().u.nvme_passthru.nbytes }
    }

    /// TODO
    #[inline]
    fn as_ref(&self) -> &spdk_bdev_io {
        unsafe { self.inner.as_ref() }
    }

    /// TODO
    #[inline]
    fn as_mut(&mut self) -> &mut spdk_bdev_io {
        unsafe { self.inner.as_mut() }
    }

    /// TODO
    #[inline]
    fn as_ptr(&self) -> *mut spdk_bdev_io {
        self.inner.as_ptr()
    }

    /// Makes a new `BdevIo` instance from a raw SPDK structure pointer.
    #[inline]
    pub(crate) fn from_ptr(bio: *mut spdk_bdev_io) -> Self {
        BdevIo {
            inner: NonNull::new(bio).unwrap(),
            _data: Default::default(),
        }
    }

    /// TODO
    #[inline]
    pub fn legacy_from_ptr(bio: *mut spdk_bdev_io) -> Self {
        Self::from_ptr(bio)
    }

    /// TODO
    #[inline]
    pub fn legacy_as_ptr(&self) -> *mut spdk_bdev_io {
        self.as_ptr()
    }
}

impl<BdevData> std::fmt::Pointer for BdevIo<BdevData>
where
    BdevData: BdevOps,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", self.as_ptr())
    }
}

impl<BdevData> Debug for BdevIo<BdevData>
where
    BdevData: BdevOps,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bdev: {} offset: {:?}, num_blocks: {:?}, type: {:?} status: {:?}, {:p} ",
            self.bdev().name(),
            self.offset(),
            self.num_blocks(),
            self.io_type(),
            self.status(),
            self.as_ptr(),
        )
    }
}

impl<BdevData> Clone for BdevIo<BdevData>
where
    BdevData: BdevOps,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            _data: Default::default(),
        }
    }
}
