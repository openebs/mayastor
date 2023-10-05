use std::{ops::Range, slice::from_raw_parts_mut};

use spdk_rs::{IoType, IoVec};

use crate::core::BlockDevice;

use super::{FaultDomain, FaultIoOperation};

/// Reference to the injection target device.
#[derive(Debug, Clone)]
pub enum InjectIoDevice {
    None,
    BlockDevice(*mut dyn BlockDevice),
    DeviceName(*const str),
}

impl From<&dyn BlockDevice> for InjectIoDevice {
    fn from(dev: &dyn BlockDevice) -> Self {
        Self::BlockDevice(dev as *const _ as *mut dyn BlockDevice)
    }
}

impl From<&str> for InjectIoDevice {
    fn from(name: &str) -> Self {
        Self::DeviceName(name as *const _)
    }
}

/// Injection I/O context.
#[derive(Debug, Clone)]
pub struct InjectIoCtx {
    pub(super) domain: FaultDomain,
    pub(super) dev: InjectIoDevice,
    pub(super) range: Range<u64>,
    pub(super) io_type: IoType,
    pub(super) iovs: *mut IoVec,
    pub(super) iovs_len: usize,
}

impl InjectIoCtx {
    /// TODO
    pub fn new(domain: FaultDomain) -> Self {
        Self {
            domain,
            dev: InjectIoDevice::None,
            range: 0 .. 0,
            io_type: IoType::Invalid,
            iovs: std::ptr::null_mut(),
            iovs_len: 0,
        }
    }

    /// TODO
    #[inline(always)]
    pub fn with_iovs<D: Into<InjectIoDevice>>(
        domain: FaultDomain,
        dev: D,
        io_type: IoType,
        offset: u64,
        num_blocks: u64,
        iovs: &[IoVec],
    ) -> Self {
        Self {
            domain,
            dev: dev.into(),
            range: offset .. offset + num_blocks,
            io_type,
            iovs: iovs.as_ptr() as *mut _,
            iovs_len: iovs.len(),
        }
    }

    /// TODO
    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        !matches!(self.dev, InjectIoDevice::None)
    }

    /// TODO
    #[inline(always)]
    pub fn domain_ok(&self, domain: FaultDomain) -> bool {
        self.domain == domain
    }

    /// Tests if the given device name matches the context's device.
    #[inline(always)]
    pub fn device_name_ok(&self, name: &str) -> bool {
        unsafe {
            match self.dev {
                InjectIoDevice::None => false,
                InjectIoDevice::BlockDevice(dev) => {
                    (*dev).device_name() == name
                }
                InjectIoDevice::DeviceName(pname) => &*pname == name,
            }
        }
    }

    /// Tests if the given fault operation matches the context's I/O.
    pub fn io_type_ok(&self, op: FaultIoOperation) -> bool {
        match op {
            FaultIoOperation::Read => self.io_type == IoType::Read,
            FaultIoOperation::Write => self.io_type == IoType::Write,
            FaultIoOperation::ReadWrite => {
                self.io_type == IoType::Read || self.io_type == IoType::Write
            }
        }
    }

    /// Tests if the range overlap with the range of the context.
    #[inline(always)]
    pub fn block_range_ok(&self, r: &Range<u64>) -> bool {
        self.range.end > r.start && r.end > self.range.start
    }

    /// TODO
    #[inline(always)]
    pub fn iovs_mut(&self) -> Option<&mut [IoVec]> {
        unsafe {
            if self.iovs.is_null()
                || !(*self.iovs).is_initialized()
                || (*self.iovs).is_empty()
                || self.iovs_len == 0
            {
                None
            } else {
                Some(from_raw_parts_mut(self.iovs, self.iovs_len))
            }
        }
    }
}
