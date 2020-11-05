use core::fmt;
use std::fmt::{Debug, Formatter};

use libc::c_void;

use spdk_sys::{
    spdk_bdev_free_io,
    spdk_bdev_io,
    spdk_bdev_io_complete,
    spdk_bdev_io_get_io_channel,
    spdk_io_channel,
};

use crate::{
    bdev::nexus::{
        nexus_bdev::{Nexus, NEXUS_PRODUCT_ID},
        nexus_fn_table::NexusFnTable,
    },
    core::Bdev,
};
use std::ptr::NonNull;

/// NioCtx provides context on a per IO basis
#[derive(Debug, Clone)]
pub struct NioCtx {
    /// read consistency
    pub(crate) in_flight: i8,
    /// status of the IO
    pub(crate) status: i32,
    /// attempts left
    pub(crate) io_attempts: i32,
}

/// BIO is a wrapper to provides a "less unsafe" wrappers around raw
/// pointers only proper scenario testing and QA cycles can determine if this
/// code is good
///
/// We have tested this on a number of underlying devices using fio and turn on
/// verification that means that each write, is read back and checked with crc2c
///
/// other testing performed is creating a mirror of two devices and deconstruct
/// the mirror and mount the individual children without a nexus driver, and use
/// filesystem checks.
///
/// # Safety
///
/// Some notes on the io pointer:
///
/// 1. The pointers are never freed rather, they are put back in to the mem
/// pool in effect accessing the pointers from rust is to be considered a
/// mutable borrow.
///
/// 2.  The IO pointers are never accessed from any other thread
/// and care must be taken that you never pass an IO ptr to another core
#[derive(Clone)]
pub(crate) struct Bio(NonNull<spdk_bdev_io>);

impl From<*mut c_void> for Bio {
    fn from(io: *mut c_void) -> Self {
        Bio(NonNull::new(io as *mut spdk_bdev_io).unwrap())
    }
}

impl From<*mut spdk_bdev_io> for Bio {
    fn from(io: *mut spdk_bdev_io) -> Self {
        Bio(NonNull::new(io as *mut spdk_bdev_io).unwrap())
    }
}

/// redefinition of IO types to make them (a) shorter and (b) get rid of the
/// enum conversion bloat.
///
/// The commented types are currently not used in our code base, uncomment as
/// needed.
pub mod io_type {
    pub const READ: u32 = 1;
    pub const WRITE: u32 = 2;
    pub const UNMAP: u32 = 3;
    //    pub const INVALID: u32 = 0;
    pub const FLUSH: u32 = 4;
    pub const RESET: u32 = 5;
    pub const NVME_ADMIN: u32 = 6;
    //    pub const NVME_IO: u32 = 7;
    //    pub const NVME_IO_MD: u32 = 8;
    pub const WRITE_ZEROES: u32 = 9;
    //    pub const ZCOPY: u32 = 10;
    //    pub const GET_ZONE_INFO: u32 = 11;
    //    pub const ZONE_MANAGMENT: u32 = 12;
    //    pub const ZONE_APPEND: u32 = 13;
    //    pub const IO_NUM_TYPES: u32 = 14;
}

/// the status of an IO - note: values copied from spdk bdev_module.h
pub mod io_status {
    //pub const NOMEM: i32 = -4;
    //pub const SCSI_ERROR: i32 = -3;
    //pub const NVME_ERROR: i32 = -2;
    pub const FAILED: i32 = -1;
    //pub const PENDING: i32 = 0;
    pub const SUCCESS: i32 = 1;
}

/// NVMe Admin opcode, from nvme_spec.h
pub mod nvme_admin_opc {
    // pub const GET_LOG_PAGE: u8 = 0x02;
    pub const IDENTIFY: u8 = 0x06;
    // pub const ABORT: u8 = 0x08;
    // pub const SET_FEATURES: u8 = 0x09;
    // pub const GET_FEATURES: u8 = 0x0a;
    // Vendor-specific
    pub const CREATE_SNAPSHOT: u8 = 0xc0;
}

impl Bio {
    /// obtain tbe Bdev this IO is associated with
    pub(crate) fn bdev_as_ref(&self) -> Bdev {
        unsafe { Bdev::from(self.0.as_ref().bdev) }
    }

    pub(crate) fn io_channel(&self) -> *mut spdk_io_channel {
        unsafe { spdk_bdev_io_get_io_channel(self.0.as_ptr()) }
    }

    /// initialize the ctx fields of an spdk_bdev_io
    pub fn init(&mut self) {
        self.ctx_as_mut_ref().io_attempts = self.nexus_as_ref().max_io_attempts;
    }

    /// reset the ctx fields of an spdk_bdev_io to submit or resubmit an IO
    pub fn reset(&mut self, in_flight: usize) {
        self.ctx_as_mut_ref().in_flight = in_flight as i8;
        self.ctx_as_mut_ref().status = io_status::SUCCESS;
    }

    /// complete an IO for the nexus. In the IO completion routine in
    /// `[nexus_bdev]` will set the IoStatus for each IO where success ==
    /// false.
    #[inline]
    pub(crate) fn ok(&mut self) {
        if cfg!(debug_assertions) {
            // have a child IO that has failed
            if self.ctx_as_mut_ref().status < 0 {
                debug!("BIO for nexus {} failed", self.nexus_as_ref().name)
            }
            // we are marking the IO done but not all child IOs have returned,
            // regardless of their state at this point
            if self.ctx_as_mut_ref().in_flight != 0 {
                debug!("BIO for nexus marked completed but has outstanding")
            }
        }
        unsafe {
            spdk_bdev_io_complete(self.0.as_ptr(), io_status::SUCCESS);
        }
    }
    /// mark the IO as failed
    #[inline]
    pub(crate) fn fail(&self) {
        unsafe {
            spdk_bdev_io_complete(self.0.as_ptr(), io_status::FAILED);
        }
    }

    /// assess the IO if we need to mark it failed or ok.
    #[inline]
    pub(crate) fn assess(&mut self, child_io: &mut Bio, success: bool) {
        self.ctx_as_mut_ref().in_flight -= 1;

        debug_assert!(self.ctx_as_mut_ref().in_flight >= 0);

        if !success {
            let io_offset = self.offset();
            let io_num_blocks = self.num_blocks();
            self.nexus_as_ref().error_record_add(
                child_io.bdev_as_ref().as_ptr(),
                self.io_type(),
                io_status::FAILED,
                io_offset,
                io_num_blocks,
            );
        }

        if self.ctx_as_mut_ref().in_flight == 0 {
            if self.ctx_as_mut_ref().status == io_status::FAILED {
                self.ctx_as_mut_ref().io_attempts -= 1;
                if self.ctx_as_mut_ref().io_attempts > 0 {
                    NexusFnTable::io_submit_or_resubmit(
                        self.io_channel(),
                        &mut self.clone(),
                    );
                } else {
                    self.fail();
                }
            } else {
                self.ok();
            }
        }
    }

    /// obtain the Nexus struct embedded within the bdev
    pub(crate) fn nexus_as_ref(&self) -> &Nexus {
        let b = self.bdev_as_ref();
        assert_eq!(b.product_name(), NEXUS_PRODUCT_ID);
        unsafe { Nexus::from_raw((*b.as_ptr()).ctxt) }
    }

    /// get the context of the given IO, which is used to determine the overall
    /// state of the IO.
    #[inline]
    pub(crate) fn ctx_as_mut_ref(&mut self) -> &mut NioCtx {
        unsafe {
            &mut *(self.0.as_mut().driver_ctx.as_mut_ptr() as *mut NioCtx)
        }
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

    /// NVMe passthru command
    #[inline]
    pub(crate) fn nvme_cmd(&self) -> spdk_sys::spdk_nvme_cmd {
        unsafe { self.0.as_ref().u.nvme_passthru.cmd }
    }

    /// raw pointer to NVMe passthru data buffer
    #[inline]
    pub(crate) fn nvme_buf(&self) -> *mut c_void {
        unsafe { self.0.as_ref().u.nvme_passthru.buf as *mut _ }
    }

    /// NVMe passthru number of bytes to transfer
    #[inline]
    pub(crate) fn nvme_nbytes(&self) -> u64 {
        unsafe { self.0.as_ref().u.nvme_passthru.nbytes }
    }

    /// free the IO
    pub(crate) fn free(&self) {
        unsafe { spdk_bdev_free_io(self.0.as_ptr()) }
    }

    /// determine the type of this IO
    #[inline]
    pub(crate) fn io_type(&self) -> u32 {
        unsafe { self.0.as_ref().type_ as u32 }
    }

    /// get the block length of this IO
    #[inline]
    pub(crate) fn block_len(&self) -> u64 {
        self.bdev_as_ref().block_len() as u64
    }
    #[inline]
    pub(crate) fn status(&self) -> i8 {
        unsafe { self.0.as_ref().internal.status }
    }

    /// determine if the IO needs an indirect buffer this can happen for example
    /// when we do a 512 write to a 4k device.
    #[inline]
    pub(crate) fn need_buf(&self) -> bool {
        unsafe {
            let slice = std::slice::from_raw_parts_mut(
                self.iovs(),
                self.iov_count() as usize,
            );

            slice[0].iov_base.is_null()
        }
    }

    pub(crate) fn as_ptr(&self) -> *mut spdk_bdev_io {
        self.0.as_ptr()
    }
}

impl Debug for Bio {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "bdev: {} offset: {:?}, num_blocks: {:?}, type: {:?} status: {:?}, {:p} ",
            self.bdev_as_ref().name(),
            self.offset(),
            self.num_blocks(),
            self.io_type(),
            self.status(),
            self
        )
    }
}
