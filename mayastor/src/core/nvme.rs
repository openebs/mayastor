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
            _ => {
                error!("unknown code {}", i);
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

impl NvmeCommandStatus {
    pub fn from_command_status(sct: i32, sc: i32) -> Self {
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
}
