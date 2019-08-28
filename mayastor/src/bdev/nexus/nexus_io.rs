//!
//! wrappers around BIO's for the nexus driver none of this code is safe

use crate::bdev::{
    nexus::nexus_bdev::{Nexus, NEXUS_PRODUCT_ID},
    Bdev,
};
use spdk_sys::{
    spdk_bdev_free_io,
    spdk_bdev_io,
    spdk_bdev_io_complete,
    SPDK_BDEV_IO_STATUS_FAILED,
    SPDK_BDEV_IO_STATUS_NOMEM,
    SPDK_BDEV_IO_STATUS_NVME_ERROR,
    SPDK_BDEV_IO_STATUS_PENDING,
    SPDK_BDEV_IO_STATUS_SCSI_ERROR,
    SPDK_BDEV_IO_STATUS_SUCCESS,
};

use libc::c_void;
use num;

/// Nexus IO is a wrapper to provides a "less unsafe" wrappers around raw
/// pointers only proper scenario testing and QA cycles can determine if this
/// code is good
///
/// We have tested this on an number of underlying devices using fio and turn on
/// verification that means that each write, is read back and checked with crc2c
///
/// other testing performed is creating a mirror of two devices and deconstruct
/// the mirror and mount the individual children without a nexus driver, and use
/// filesystem checks.
#[derive(Debug)]
pub(crate) struct Nio {
    pub io: *mut spdk_bdev_io,
}

#[derive(FromPrimitive, Debug)]
pub enum NioType {
    /// an invalid IO type
    Invalid = 0,
    /// READ IO
    Read,
    /// WRITE IO
    Write,
    /// UNMAP
    Unmap,
    /// FLUSH
    Flush,
    /// RESET
    Reset,
    /// NVME admin command used during passtru
    NvmeAdmin,
    /// same as above but for regular IO
    NvmeIo,
    /// Metadata IO used for guards
    NvmeIoMd,
    /// writezeros to erase data on disk
    WriteZeroes,
    /// zero copy IO
    Zcopy,
    /// the number of IOs
    NumTypes = 11,
}

impl From<i32> for NioType {
    fn from(io: i32) -> Self {
        num::FromPrimitive::from_i32(io).unwrap()
    }
}

impl From<u32> for NioType {
    fn from(io: u32) -> Self {
        num::FromPrimitive::from_u32(io).unwrap()
    }
}
/// IOStatus i32 in SPDK all non error states are negative
#[derive(FromPrimitive, PartialEq, ToPrimitive, Debug)]
pub(crate) enum IoStatus {
    Pending = SPDK_BDEV_IO_STATUS_PENDING as isize,
    Success = SPDK_BDEV_IO_STATUS_SUCCESS as isize,
    Failed = SPDK_BDEV_IO_STATUS_FAILED as isize,
    NvmeError = SPDK_BDEV_IO_STATUS_NVME_ERROR as isize,
    ScsiError = SPDK_BDEV_IO_STATUS_SCSI_ERROR as isize,
    NoMemory = SPDK_BDEV_IO_STATUS_NOMEM as isize,
}

impl From<*mut spdk_bdev_io> for Nio {
    fn from(io: *mut spdk_bdev_io) -> Self {
        Nio {
            io,
        }
    }
}

impl From<*mut c_void> for Nio {
    fn from(io: *mut c_void) -> Self {
        Nio {
            io: io as *const _ as *mut _,
        }
    }
}

impl Nio {
    /// obtain tbe Bdev this IO is associated with
    pub(crate) fn bdev_as_ref(&self) -> Bdev {
        unsafe { Bdev::from((*self.io).bdev) }
    }

    /// obtain the Nexus struct embedded within the bdev
    pub(crate) fn nexus_as_ref(&self) -> &Nexus {
        let b = self.bdev_as_ref();
        assert_eq!(b.product_name(), NEXUS_PRODUCT_ID);
        unsafe { Nexus::from_raw((*b.inner).ctxt) }
    }

    /// complete the IO of the nexus depending on the state of the child IOs.
    /// Right now, this is very simplistic. In the future we intent to implement
    /// different policies based on intent. For example; create a policy where
    /// we want each READ IO to be read of all 3 mirrors and verified before
    /// returned to the user. Or we can say, for writes, write out a
    /// majority of children and then return

    //#[inline]
    pub(crate) fn io_complete(&mut self, status: IoStatus) {
        // update the status of the current Nexus IO
        self.nio_set_status(status);
        // get the policy based determination if the IO is completed
        if self.outstanding_completed() {
            // get the actual state of the completed IO and send up the chain
            let nio_status = self.nio_get_status();
            unsafe {
                spdk_bdev_io_complete(
                    self.io,
                    num::ToPrimitive::to_i32(&nio_status).unwrap(),
                )
            }
        }
    }

    /// obtain a mut slice to the driver ctx. When this structure requires more
    /// space then an u8, we need to change this signature to *mut T and call
    /// .as_mut_ptr()
    //#[inline]
    pub(crate) fn get_io_private(&mut self) -> &mut [u8] {
        unsafe { (*self.io).driver_ctx.as_mut_slice(1) }
    }

    /// set the status of the nexus IO, typically its a one to one mapping of
    /// the child IO. However, based on policy a failed child IO does not
    /// always imply a failed nexus IO
    //#[inline]
    pub(crate) fn nio_set_status(&mut self, status: IoStatus) {
        unsafe { (*self.io).u.bdev.split_outstanding -= 1 };
        let io_private = self.get_io_private();
        io_private[0] = num::ToPrimitive::to_i8(&status).unwrap() as u8;
    }

    /// get the "calculated" state of the IO
    //#[inline]
    pub(crate) fn nio_get_status(&mut self) -> IoStatus {
        let io_private = self.get_io_private();
        num::FromPrimitive::from_i8(io_private[0] as i8).unwrap()
    }

    /// set the total number of child ios associated with this nexus IO
    //#[inline]
    pub(crate) fn set_outstanding(&mut self, i: usize) {
        unsafe { (*self.io).u.bdev.split_outstanding = 1 + i as u32 };
        self.nio_set_status(IoStatus::Success)
    }

    /// determine if all the child IOS have completed. Depending on the policy
    #[inline]
    pub(crate) fn outstanding_completed(&mut self) -> bool {
        unsafe { (*self.io).u.bdev.split_outstanding == 0 }
    }

    /// get a raw pointer to the base of the iov
    #[inline]
    pub(crate) fn iovs(&self) -> *mut spdk_sys::iovec {
        unsafe { (*self.io).u.bdev.iovs }
    }

    /// number of iovs that are part of this IO
    #[inline]
    pub(crate) fn iov_count(&self) -> i32 {
        unsafe { (*self.io).u.bdev.iovcnt }
    }

    /// offset where we do the IO on the device
    #[inline]
    pub(crate) fn offset(&self) -> u64 {
        unsafe { (*self.io).u.bdev.offset_blocks }
    }

    /// num of blocks this IO will read/write/unmap
    #[inline]
    pub(crate) fn num_blocks(&self) -> u64 {
        unsafe { (*self.io).u.bdev.num_blocks }
    }

    /// free the io directly without completion note that the IO is not freed
    /// but rather put back into the mempool which is allocated during startup
    #[inline]
    pub(crate) fn io_free(io: *mut spdk_bdev_io) {
        unsafe { spdk_bdev_free_io(io) }
    }

    /// determine the type of this IO
    #[inline]
    pub(crate) fn io_type(io: *mut spdk_bdev_io) -> Option<NioType> {
        unsafe { num::FromPrimitive::from_u8((*io).type_) }
    }

    /// get the block length of this IO
    #[inline]
    pub(crate) fn block_len(&self) -> u64 {
        unsafe { u64::from((*(*self.io).bdev).blocklen) }
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
}
