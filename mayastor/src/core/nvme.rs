use crate::bdev::Bio;
use spdk_sys::spdk_bdev_io_get_nvme_status;

#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum GenericStatusCode {
    Success,
    InvalidOpcode,
    InternalDeviceError,
    AbortedRequested,
    Reserved,
    AbortedSubmissionQueueDeleted,
}

impl From<i32> for GenericStatusCode {
    fn from(i: i32) -> Self {
        match i {
            0x00 => Self::Success,
            0x01 => Self::InvalidOpcode,
            0x06 => Self::InternalDeviceError,
            0x07 => Self::AbortedRequested,
            0x08 => Self::AbortedSubmissionQueueDeleted,
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
    sct: i32,
    /// NVMe status code
    sc: GenericStatusCode,
}

impl NvmeStatus {
    pub fn status_code(&self) -> GenericStatusCode {
        self.sc
    }
    // todo make enums
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
            sct,
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
            sct,
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
            sct,
            sc: GenericStatusCode::from(sc),
        }
    }
}
