use core::fmt;
use std::{
    fmt::{Debug, Formatter},
    ptr::NonNull,
};

use libc::c_void;

use spdk_sys::{
    spdk_bdev_free_io,
    spdk_bdev_io,
    spdk_bdev_io_complete,
    spdk_bdev_io_get_io_channel,
    spdk_io_channel,
};

use crate::{
    bdev::{
        nexus::{
            nexus_bdev::{Nexus, NEXUS_PRODUCT_ID},
            nexus_channel::DREvent,
            nexus_fn_table::NexusFnTable,
        },
        nexus_lookup,
        ChildState,
        NexusStatus,
        Reason,
    },
    core::{Bdev, Cores, GenericStatusCode, Mthread, NvmeStatus, Reactors},
    nexus_uri::bdev_destroy,
};

/// NioCtx provides context on a per IO basis
#[derive(Debug, Clone)]
pub struct NioCtx {
    /// read consistency
    pub(crate) in_flight: i8,
    /// status of the IO
    pub(crate) status: IoStatus,
    /// attempts left
    pub(crate) io_attempts: i32,
}

impl NioCtx {
    #[inline]
    pub fn dec(&mut self) {
        self.in_flight -= 1;
        debug_assert!(self.in_flight >= 0);
    }
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
pub struct Bio(NonNull<spdk_bdev_io>);

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

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Eq)]
pub enum IoType {
    Invalid,
    Read,
    Write,
    Unmap,
    Flush,
    Reset,
    NvmeAdmin,
    NvmeIO,
    NvmeIOMD,
    WriteZeros,
    ZeroCopy,
    ZoneInfo,
    ZoneManagement,
    ZoneAppend,
    Compare,
    CompareAndWrite,
    Abort,
    IoNumTypes,
}

impl From<IoType> for u32 {
    fn from(t: IoType) -> Self {
        match t {
            IoType::Invalid => 0,
            IoType::Read => 1,
            IoType::Write => 2,
            IoType::Unmap => 3,
            IoType::Flush => 4,
            IoType::Reset => 5,
            IoType::NvmeAdmin => 6,
            IoType::NvmeIO => 7,
            IoType::NvmeIOMD => 8,
            IoType::WriteZeros => 9,
            IoType::ZeroCopy => 10,
            IoType::ZoneInfo => 11,
            IoType::ZoneManagement => 12,
            IoType::ZoneAppend => 13,
            IoType::Compare => 14,
            IoType::CompareAndWrite => 15,
            IoType::Abort => 16,
            IoType::IoNumTypes => 17,
        }
    }
}

impl From<u32> for IoType {
    fn from(u: u32) -> Self {
        match u {
            0 => Self::Invalid,
            1 => Self::Read,
            2 => Self::Write,
            3 => Self::Unmap,
            4 => Self::Flush,
            5 => Self::Reset,
            6 => Self::NvmeAdmin,
            7 => Self::NvmeIO,
            8 => Self::NvmeIOMD,
            9 => Self::WriteZeros,
            10 => Self::ZeroCopy,
            11 => Self::ZoneInfo,
            12 => Self::ZoneManagement,
            13 => Self::ZoneAppend,
            14 => Self::Compare,
            15 => Self::CompareAndWrite,
            16 => Self::Abort,
            17 => Self::IoNumTypes,
            _ => panic!("invalid IO type"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Eq)]
#[non_exhaustive]
pub enum IoStatus {
    Aborted,
    FirstFusedFailed,
    MisCompared,
    NoMemory,
    ScsiError,
    NvmeError,
    Failed,
    Pending,
    Success,
}

impl From<i32> for IoStatus {
    fn from(status: i32) -> Self {
        match status {
            -7 => Self::Aborted,
            -6 => Self::FirstFusedFailed,
            -5 => Self::MisCompared,
            -4 => Self::NoMemory,
            -3 => Self::ScsiError,
            -2 => Self::NvmeError,
            -1 => Self::Failed,
            0 => Self::Pending,
            1 => Self::Success,
            _ => panic!("invalid status code"),
        }
    }
}

impl From<IoStatus> for i32 {
    fn from(i: IoStatus) -> Self {
        match i {
            IoStatus::Aborted => -7,
            IoStatus::FirstFusedFailed => -6,
            IoStatus::MisCompared => -5,
            IoStatus::NoMemory => -4,
            IoStatus::ScsiError => -3,
            IoStatus::NvmeError => -2,
            IoStatus::Failed => -1,
            IoStatus::Pending => 0,
            IoStatus::Success => 1,
        }
    }
}

impl From<i8> for IoStatus {
    fn from(status: i8) -> Self {
        (status as i32).into()
    }
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
        self.ctx_as_mut_ref().status = IoStatus::Success;
    }

    /// complete an IO for the nexus. In the IO completion routine in
    /// `[nexus_bdev]` will set the IoStatus for each IO where success ==
    /// false.
    #[inline]
    pub(crate) fn ok(&mut self) {
        if cfg!(debug_assertions) {
            // have a child IO that has failed
            if self.ctx_as_mut_ref().status != IoStatus::Success {
                debug!("BIO for nexus {} failed", self.nexus_as_ref().name)
            }
            // we are marking the IO done but not all child IOs have returned,
            // regardless of their state at this point
            if self.ctx_as_mut_ref().in_flight != 0 {
                debug!("BIO for nexus marked completed but has outstanding")
            }
        }
        unsafe {
            spdk_bdev_io_complete(self.0.as_ptr(), IoStatus::Success.into())
        }
    }
    /// mark the IO as failed
    #[inline]
    pub(crate) fn fail(&self) {
        unsafe {
            spdk_bdev_io_complete(self.0.as_ptr(), IoStatus::Failed.into())
        }
    }

    #[inline]
    pub(crate) fn complete(&mut self) {
        let pio_ctx = self.ctx_as_mut_ref();
        if pio_ctx.in_flight == 0 {
            if pio_ctx.status == IoStatus::Failed {
                pio_ctx.io_attempts -= 1;
                if pio_ctx.io_attempts > 0 {
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

    /// assess the IO if we need to mark it failed or ok.
    #[inline]
    pub(crate) fn assess(&mut self, child_io: &mut Bio, success: bool) {
        self.ctx_as_mut_ref().dec();

        if !success {
            // currently, only tests send those but invalid op codes should not
            // result into faulting a child device.
            if NvmeStatus::from(child_io.clone()).status_code()
                == GenericStatusCode::InvalidOpcode
            {
                self.complete();
                return;
            }

            // all other status codes indicate a fatal error
            Reactors::master().send_future(Self::child_retire(
                self.nexus_as_ref().name.clone(),
                child_io.bdev_as_ref(),
            ));
        }

        self.complete();
    }

    async fn child_retire(nexus: String, child: Bdev) {
        error!("{:#?}", child);

        if let Some(nexus) = nexus_lookup(&nexus) {
            if let Some(child) = nexus.child_lookup(&child.name()) {
                let current_state = child.state.compare_and_swap(
                    ChildState::Open,
                    ChildState::Faulted(Reason::IoError),
                );

                if current_state == ChildState::Open {
                    warn!(
                        "core {} thread {:?}, faulting child {}",
                        Cores::current(),
                        Mthread::current(),
                        child,
                    );

                    let uri = child.name.clone();
                    nexus.pause().await.unwrap();
                    nexus.reconfigure(DREvent::ChildFault).await;
                    //nexus.remove_child(&uri).await.unwrap();

                    // Note, an error can occur here if a separate task,
                    // e.g. grpc request is also deleting the child,
                    // in which case the bdev may no longer exist at
                    // this point. To be addressed by CAS-632 to
                    // improve synchronization.
                    if let Err(err) = bdev_destroy(&uri).await {
                        error!("{} destroying bdev {}", err, uri)
                    }

                    nexus.resume().await.unwrap();
                    if nexus.status() == NexusStatus::Faulted {
                        error!(":{} has no children left... ", nexus);
                    }
                }
            }
        } else {
            debug!("{} does not belong (anymore) to nexus {}", child, nexus);
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
    pub(crate) fn io_type(&self) -> IoType {
        unsafe { self.0.as_ref().type_ as u32 }.into()
    }

    /// get the block length of this IO
    #[inline]
    pub(crate) fn block_len(&self) -> u64 {
        self.bdev_as_ref().block_len() as u64
    }
    #[inline]
    pub(crate) fn status(&self) -> IoStatus {
        unsafe { self.0.as_ref().internal.status }.into()
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
