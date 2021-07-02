use crate::core::{
    nvme::StatusCodeType::{
        CommandSpecificStatus,
        GenericCommandStatus,
        MediaDataIntegrityErrors,
        Reserved,
        VendorSpecific,
    },
    Bio,
};
use spdk_sys::spdk_bdev_io_get_nvme_status;

#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum StatusCodeType {
    GenericCommandStatus,
    CommandSpecificStatus,
    MediaDataIntegrityErrors,
    Reserved,
    VendorSpecific,
}

impl From<i32> for StatusCodeType {
    fn from(i: i32) -> Self {
        match i {
            0x00 => GenericCommandStatus,
            0x01 => CommandSpecificStatus,
            0x02 => MediaDataIntegrityErrors,
            0x07 => VendorSpecific,
            _ => Reserved,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub enum GenericStatusCode {
    Success,
    InvalidOpcode,
    InvalidFieldInCommand,
    CommandIDConflict,
    DataTransferError,
    CommandsAbortedDueToPowerLoss,
    InternalDeviceError,
    AbortedRequested,
    AbortedSubmissionQueueDeleted,
    AbortedSubmissionFailedFusedCommand,
    AbortedSubmissionMissingFusedCommand,
    InvalidNameSpaceOrFormat,
    CommandSequenceError,
    InvalidSGLDescriptor,
    InvalidNumberOfSGLDescriptors,
    DataSGLLengthInvalid,
    MetaDataSGLLengthInvalid,
    SGLTypeDescriptorInvalid,
    InvalidUseOfControlMemoryBuffer,
    PRPOffsetInvalid,
    AtomicWriteUnitExceeded,
    OperationDenied,
    SGLOffsetInvalid,
    HostIdentifierInvalidFormat,
    KATOExpired,
    KATOInvalid,
    CommandAbortPreemt,
    SanitizeFailed,
    SanitizeInProgress,
    SGLDataBlockGranularityInvalid,
    CommandInvalidInCMB,
    LBAOutOfRange,
    CapacityExceeded,
    NamespaceNotReady,
    ReservationConflict,
    FormatInProgress,
    Reserved,
}
#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum NvmeCommandStatus {
    CommandSpecificStatus,
    GenericCommandStatus(GenericStatusCode),
    MediaDataIntegrityErrors,
    VendorSpecific,
    Reserved,
}

impl From<i32> for GenericStatusCode {
    fn from(i: i32) -> Self {
        match i {
            0x00 => Self::Success,
            0x01 => Self::InvalidOpcode,
            0x02 => Self::InvalidFieldInCommand,
            0x03 => Self::CommandIDConflict,
            0x04 => Self::DataTransferError,
            0x05 => Self::CommandsAbortedDueToPowerLoss,
            0x06 => Self::InternalDeviceError,
            0x07 => Self::AbortedRequested,
            0x08 => Self::AbortedSubmissionQueueDeleted,
            0x09 => Self::AbortedSubmissionFailedFusedCommand,
            0x0A => Self::AbortedSubmissionMissingFusedCommand,
            0x0B => Self::InvalidNameSpaceOrFormat,
            0x0C => Self::CommandSequenceError,
            0x0D => Self::InvalidSGLDescriptor,
            0x0E => Self::InvalidSGLDescriptor,
            0x0F => Self::DataSGLLengthInvalid,
            0x10 => Self::MetaDataSGLLengthInvalid,
            0x11 => Self::SGLTypeDescriptorInvalid,
            0x12 => Self::InvalidUseOfControlMemoryBuffer,
            0x13 => Self::PRPOffsetInvalid,
            0x14 => Self::AtomicWriteUnitExceeded,
            0x15 => Self::OperationDenied,
            0x16 => Self::SGLOffsetInvalid,
            0x17 => Self::Reserved,
            0x18 => Self::HostIdentifierInvalidFormat,
            0x19 => Self::KATOExpired,
            0x1A => Self::KATOInvalid,
            0x1B => Self::CommandAbortPreemt,
            0x1C => Self::SanitizeFailed,
            0x1D => Self::SanitizeInProgress,
            0x1E => Self::SGLDataBlockGranularityInvalid,
            0x1F => Self::CommandInvalidInCMB,
            0x80 => Self::LBAOutOfRange,
            0x81 => Self::CapacityExceeded,
            0x82 => Self::NamespaceNotReady,
            0x83 => Self::ReservationConflict,
            0x84 => Self::FormatInProgress,
            _ => {
                error!("unknown code {:x}", i);
                Self::Reserved
            }
        }
    }
}

#[derive(Debug)]
pub struct NvmeStatus {
    /// NVMe completion queue entry
    cdw0: u32,
    /// NVMe status code type
    sct: StatusCodeType,
    /// NVMe status code
    sc: GenericStatusCode,
}

impl NvmeStatus {
    pub fn status_code(&self) -> GenericStatusCode {
        self.sc
    }
    pub fn status_type(&self) -> StatusCodeType {
        self.sct
    }
}

impl From<Bio> for NvmeStatus {
    fn from(b: Bio) -> Self {
        let mut cdw0: u32 = 0;
        let mut sct: i32 = 0;
        let mut sc: i32 = 0;

        unsafe {
            spdk_bdev_io_get_nvme_status(
                b.as_ptr(),
                &mut cdw0,
                &mut sct,
                &mut sc,
            )
        }

        Self {
            cdw0,
            sct: StatusCodeType::from(sct),
            sc: GenericStatusCode::from(sc),
        }
    }
}

impl From<&mut Bio> for NvmeStatus {
    fn from(b: &mut Bio) -> Self {
        let mut cdw0: u32 = 0;
        let mut sct: i32 = 0;
        let mut sc: i32 = 0;

        unsafe {
            spdk_bdev_io_get_nvme_status(
                b.as_ptr(),
                &mut cdw0,
                &mut sct,
                &mut sc,
            )
        }

        Self {
            cdw0,
            sct: StatusCodeType::from(sct),
            sc: GenericStatusCode::from(sc),
        }
    }
}
impl From<&Bio> for NvmeStatus {
    fn from(b: &Bio) -> Self {
        let mut cdw0: u32 = 0;
        let mut sct: i32 = 0;
        let mut sc: i32 = 0;

        unsafe {
            spdk_bdev_io_get_nvme_status(
                b.as_ptr(),
                &mut cdw0,
                &mut sct,
                &mut sc,
            )
        }

        Self {
            cdw0,
            sct: StatusCodeType::from(sct),
            sc: GenericStatusCode::from(sc),
        }
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

/// NVM command set opcodes, from nvme_spec.h
pub mod nvme_nvm_opcode {
    // pub const FLUSH: u8 = 0x00;
    // pub const WRITE: u8 = 0x01;
    // pub const READ: u8 = 0x02;
    // pub const WRITE_UNCORRECTABLE: u8 = 0x04;
    // pub const COMPARE: u8 = 0x05;
    // pub const WRITE_ZEROES: u8 = 0x08;
    // pub const DATASET_MANAGEMENT: u8 = 0x09;
    pub const RESERVATION_REGISTER: u8 = 0x0d;
    pub const RESERVATION_REPORT: u8 = 0x0e;
    pub const RESERVATION_ACQUIRE: u8 = 0x11;
    // pub const RESERVATION_RELEASE: u8 = 0x15;
}

pub mod nvme_reservation_type {
    pub const WRITE_EXCLUSIVE: u8 = 0x1;
    pub const EXCLUSIVE_ACCESS: u8 = 0x2;
    pub const WRITE_EXCLUSIVE_REG_ONLY: u8 = 0x3;
    pub const EXCLUSIVE_ACCESS_REG_ONLY: u8 = 0x4;
    pub const WRITE_EXCLUSIVE_ALL_REGS: u8 = 0x5;
    pub const EXCLUSIVE_ACCESS_ALL_REGS: u8 = 0x6;
}

pub mod nvme_reservation_register_action {
    pub const REGISTER_KEY: u8 = 0x0;
    pub const UNREGISTER_KEY: u8 = 0x1;
    pub const REPLACE_KEY: u8 = 0x2;
}

pub mod nvme_reservation_register_cptpl {
    pub const NO_CHANGES: u8 = 0x0;
    pub const CLEAR_POWER_ON: u8 = 0x2;
    pub const PERSIST_POWER_LOSS: u8 = 0x2;
}

pub mod nvme_reservation_acquire_action {
    pub const ACQUIRE: u8 = 0x0;
    pub const PREEMPT: u8 = 0x1;
    pub const PREEMPT_ABORT: u8 = 0x2;
}

impl NvmeCommandStatus {
    pub fn from_command_status_raw(sct: i32, sc: i32) -> Self {
        match StatusCodeType::from(sct) {
            CommandSpecificStatus => Self::CommandSpecificStatus,
            GenericCommandStatus => {
                Self::GenericCommandStatus(GenericStatusCode::from(sc))
            }
            MediaDataIntegrityErrors => Self::MediaDataIntegrityErrors,
            VendorSpecific => Self::VendorSpecific,
            _ => Self::Reserved,
        }
    }
    pub fn from_command_status(
        sct: StatusCodeType,
        sc: GenericStatusCode,
    ) -> Self {
        match sct {
            CommandSpecificStatus => Self::CommandSpecificStatus,
            GenericCommandStatus => Self::GenericCommandStatus(sc),
            MediaDataIntegrityErrors => Self::MediaDataIntegrityErrors,
            VendorSpecific => Self::VendorSpecific,
            _ => Self::Reserved,
        }
    }
}
