#![cfg(feature = "fault-injection")]

use snafu::Snafu;
use std::{
    convert::TryFrom,
    fmt::{Display, Formatter},
    ops::Range,
    slice::from_raw_parts_mut,
    time::Duration,
};
use url::ParseError;

mod injection;
mod injections;

use crate::core::{BlockDevice, IoCompletionStatus};
pub use injection::FaultInjection;
pub use injections::{
    add_fault_injection,
    inject_completion_error,
    inject_submission_error,
    list_fault_injections,
    remove_fault_injection,
};
use spdk_rs::{IoType, IoVec};

/// Fault domain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FaultDomain {
    None,
    Nexus,
    BlockDevice,
}

impl Display for FaultDomain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Nexus => "nexus",
            Self::BlockDevice => "block_device",
        })
    }
}

/// Data fault mode.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataFaultMode {
    Rand,
}

impl Display for DataFaultMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Rand => "rand",
        })
    }
}

/// Fault I/O type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FaultIoType {
    Read,
    Write,
}

impl Display for FaultIoType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read => f.write_str("read"),
            Self::Write => f.write_str("write"),
        }
    }
}

impl TryFrom<IoType> for FaultIoType {
    type Error = ();

    fn try_from(value: IoType) -> Result<Self, Self::Error> {
        match value {
            IoType::Read => Ok(Self::Read),
            IoType::Write => Ok(Self::Write),
            _ => Err(()),
        }
    }
}

/// Fault I/O stage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FaultIoStage {
    Submission,
    Completion,
}

impl Display for FaultIoStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Submission => f.write_str("submit"),
            Self::Completion => f.write_str("compl"),
        }
    }
}

/// Fault type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FaultType {
    Status(IoCompletionStatus),
    Data,
}

impl Display for FaultType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(_) => f.write_str("status"),
            Self::Data => f.write_str("data"),
        }
    }
}

impl FaultType {
    pub fn status_data_transfer_error() -> Self {
        use spdk_rs::{GenericStatusCode, NvmeStatus};

        Self::Status(IoCompletionStatus::NvmeError(NvmeStatus::Generic(
            GenericStatusCode::DataTransferError,
        )))
    }
}

/// Injection I/O.
#[derive(Debug, Clone)]
pub struct InjectIoCtx {
    dev: Option<*mut dyn BlockDevice>,
    io_type: IoType,
    range: Range<u64>,
    iovs: *mut IoVec,
    iovs_len: usize,
}

impl Default for InjectIoCtx {
    fn default() -> Self {
        Self {
            dev: None,
            io_type: IoType::Invalid,
            range: 0 .. 0,
            iovs: std::ptr::null_mut(),
            iovs_len: 0,
        }
    }
}

impl InjectIoCtx {
    /// TODO
    #[inline(always)]
    pub fn with_iovs(
        dev: &dyn BlockDevice,
        io_type: IoType,
        offset: u64,
        num_blocks: u64,
        iovs: &[IoVec],
    ) -> Self {
        Self {
            dev: Some(dev as *const _ as *mut dyn BlockDevice),
            io_type,
            range: offset .. offset + num_blocks,
            iovs: iovs.as_ptr() as *mut _,
            iovs_len: iovs.len(),
        }
    }

    /// TODO
    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.dev.is_some()
    }

    /// TODO
    #[inline(always)]
    pub fn device(&self) -> &dyn BlockDevice {
        unsafe { &*self.dev.unwrap() }
    }

    /// TODO
    #[inline(always)]
    pub fn device_name(&self) -> String {
        self.device().device_name()
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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum FaultInjectionError {
    #[snafu(display("Injections are disabled"))]
    InjectionsDisabled {},
    #[snafu(display("URI is not an injection: '{}'", uri))]
    NotInjectionUri { uri: String },
    #[snafu(display("Invalid injection URI: '{}'", uri))]
    InvalidUri { source: ParseError, uri: String },
    #[snafu(display("Unknown injection parameter: '{}={}'", name, value))]
    UnknownParameter { name: String, value: String },
    #[snafu(display("Bad injection parameter value: '{}={}'", name, value))]
    BadParameterValue { name: String, value: String },
    #[snafu(display(
        "Bad injection '{}' timer durations: {:?}, {:?}",
        name,
        begin,
        end
    ))]
    BadDurations {
        name: String,
        begin: Duration,
        end: Duration,
    },
}
