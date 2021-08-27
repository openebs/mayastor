use core::fmt;
use std::{
    fmt::{Debug, Formatter},
    ptr::NonNull,
};

use libc::c_void;

use spdk_sys::{spdk_bdev_io, spdk_bdev_io_complete};

use crate::{
    bdev::nexus::nexus_bdev::{Nexus, NEXUS_PRODUCT_ID},
    core::{Bdev, NvmeStatus},
};

use spdk::{IoStatus, IoType};

#[derive(Clone)]
#[repr(transparent)]
pub struct Bio(NonNull<spdk_bdev_io>);

/// NOT safe
impl From<*mut c_void> for Bio {
    fn from(io: *mut c_void) -> Self {
        Bio(NonNull::new(io as *mut spdk_bdev_io).expect("null ptr"))
    }
}

/// NOT safe
impl From<*mut spdk_bdev_io> for Bio {
    fn from(io: *mut spdk_bdev_io) -> Self {
        Bio(NonNull::new(io as *mut spdk_bdev_io).expect("null ptr"))
    }
}

impl Bio {
    /// obtain the Bdev this IO is associated with
    pub(crate) fn bdev(&self) -> Bdev {
        unsafe { Bdev::from(self.0.as_ref().bdev) }
    }

    #[inline]
    pub(crate) fn ok(&self) {
        unsafe {
            spdk_bdev_io_complete(self.0.as_ptr(), IoStatus::Success.into())
        }
    }

    /// mark the IO as failed
    #[inline]
    pub(crate) fn fail(&self) {
        unsafe {
            trace!(?self, "failed");
            spdk_bdev_io_complete(self.0.as_ptr(), IoStatus::Failed.into())
        }
    }

    /// mark the IO as impossible to submit due to a memory constraint
    #[inline]
    pub(crate) fn no_mem(&self) {
        unsafe {
            spdk_bdev_io_complete(self.0.as_ptr(), IoStatus::NoMemory.into())
        }
    }

    /// assess the IO if we need to mark it failed or ok.
    /// obtain the Nexus struct embedded within the bdev
    pub(crate) fn nexus_as_ref(&self) -> &Nexus {
        let b = self.bdev();
        assert_eq!(b.product_name(), NEXUS_PRODUCT_ID);
        unsafe { Nexus::from_raw((*b.as_ptr()).ctxt) }
    }

    /// get the context specifics of this IO
    #[inline]
    pub(crate) fn specific_as_mut<T>(&mut self) -> &mut T {
        unsafe { &mut *(self.0.as_mut().driver_ctx.as_mut_ptr() as *mut T) }
    }

    /// get the IO context specific information for this IO
    #[inline]
    pub(crate) fn specific<T>(&self) -> &T {
        unsafe { &*(self.0.as_ref().driver_ctx.as_ptr() as *const T) }
    }

    /// get a raw pointer to the base of the iov
    #[inline]
    pub(crate) fn iovs(&self) -> *mut spdk_sys::iovec {
        unsafe { self.0.as_ref().u.bdev.iovs }
    }

    /// number of iovs that are part of this IO
    #[inline]
    pub(crate) fn iov_count(&self) -> i32 {
        unsafe { self.0.as_ref().u.bdev.iovcnt }
    }

    /// offset where we do the IO on the device
    #[inline]
    pub(crate) fn offset(&self) -> u64 {
        unsafe { self.0.as_ref().u.bdev.offset_blocks }
    }

    /// num of blocks this IO will read/write/unmap
    #[inline]
    pub(crate) fn num_blocks(&self) -> u64 {
        unsafe { self.0.as_ref().u.bdev.num_blocks }
    }

    /// determine the type of this IO
    #[inline]
    pub(crate) fn io_type(&self) -> IoType {
        unsafe { self.0.as_ref().type_ as u32 }.into()
    }

    #[inline]
    pub(crate) fn status(&self) -> IoStatus {
        unsafe { self.0.as_ref().internal.status }.into()
    }

    /// get the block length of this IO
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn block_len(&self) -> u64 {
        self.bdev().block_len() as u64
    }

    /// NVMe passthru command
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn nvme_cmd(&self) -> spdk_sys::spdk_nvme_cmd {
        unsafe { self.0.as_ref().u.nvme_passthru.cmd }
    }

    /// raw pointer to NVMe passthru data buffer
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn nvme_buf(&self) -> *mut c_void {
        unsafe { self.0.as_ref().u.nvme_passthru.buf as *mut _ }
    }

    /// NVMe passthru number of bytes to transfer
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn nvme_nbytes(&self) -> u64 {
        unsafe { self.0.as_ref().u.nvme_passthru.nbytes }
    }
    /// determine if the IO needs an indirect buffer this can happen for example
    /// when we do a 512 write to a 4k device.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn need_buf(&self) -> bool {
        unsafe {
            let slice = std::slice::from_raw_parts_mut(
                self.iovs(),
                self.iov_count() as usize,
            );

            slice[0].iov_base.is_null()
        }
    }

    pub(crate) fn nvme_status(&self) -> NvmeStatus {
        NvmeStatus::from(self)
    }

    pub(crate) fn as_ptr(&self) -> *mut spdk_bdev_io {
        self.0.as_ptr()
    }
}

impl std::fmt::Pointer for Bio {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", unsafe { self.0.as_ref() })
    }
}
impl Debug for Bio {
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
