#![cfg(feature = "fault-injection")]

use snafu::Snafu;
use std::{
    fmt::{Debug, Display, Formatter},
    time::Duration,
};
use url::ParseError;

mod bdev_io_injection;
mod fault_method;
mod inject_io_ctx;
mod injection;
mod injection_api;
mod injection_state;

use bdev_io_injection::add_bdev_io_injection;
pub use fault_method::FaultMethod;
pub use inject_io_ctx::{InjectIoCtx, InjectIoDevice};
pub use injection::{Injection, InjectionBuilder, InjectionBuilderError};
pub use injection_api::{
    add_fault_injection,
    inject_completion_error,
    inject_submission_error,
    list_fault_injections,
    remove_fault_injection,
};
pub use injection_state::InjectionState;

/// Fault domain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FaultDomain {
    /// Fault injection on nexus child I/O level.
    NexusChild,
    /// Fault injection on block device abstraction level.
    BlockDevice,
    ///
    BdevIo,
}

impl Display for FaultDomain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultDomain::NexusChild => f.write_str("child"),
            FaultDomain::BlockDevice => f.write_str("block"),
            FaultDomain::BdevIo => f.write_str("bdev_io"),
        }
    }
}

/// I/O operation to which the fault applies.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FaultIoOperation {
    Read,
    Write,
    ReadWrite,
}

impl Display for FaultIoOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultIoOperation::Read => f.write_str("r"),
            FaultIoOperation::Write => f.write_str("w"),
            FaultIoOperation::ReadWrite => f.write_str("rw"),
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
            FaultIoStage::Submission => f.write_str("submit"),
            FaultIoStage::Completion => f.write_str("compl"),
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
    #[snafu(display("Injection device not found: '{name}'"))]
    DeviceNotFound { name: String },
    #[snafu(display("Injection is invalid for '{name}': {msg}"))]
    InvalidInjection { name: String, msg: String },
}
