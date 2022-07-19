use crate::{bdev_api::BdevError, core::CoreError};
use snafu::Snafu;
use spdk_rs::DmaError;

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
#[allow(missing_docs)]
/// Various rebuild errors when interacting with a rebuild job or
/// encountered during a rebuild copy
pub enum RebuildError {
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Failed to validate rebuild job creation parameters"))]
    InvalidParameters {},
    #[snafu(display("Failed to get a handle for bdev {}", bdev))]
    NoBdevHandle { source: CoreError, bdev: String },
    #[snafu(display("Bdev {} not found", bdev))]
    BdevNotFound { source: CoreError, bdev: String },
    #[snafu(display("IO failed for bdev {}", bdev))]
    IoFailed { source: CoreError, bdev: String },
    #[snafu(display("Read IO failed for bdev {}", bdev))]
    ReadIoFailed { source: CoreError, bdev: String },
    #[snafu(display("Write IO failed for bdev {}", bdev))]
    WriteIoFailed { source: CoreError, bdev: String },
    #[snafu(display("Failed to find rebuild job {}", job))]
    JobNotFound { job: String },
    #[snafu(display("Missing rebuild destination {}", job))]
    MissingDestination { job: String },
    #[snafu(display(
        "{} operation failed because current rebuild state is {}.",
        operation,
        state,
    ))]
    OpError { operation: String, state: String },
    #[snafu(display("Existing pending state {}", state,))]
    StatePending { state: String },
    #[snafu(display(
        "Failed to lock LBA range for blk {}, len {}, with error: {}",
        blk,
        len,
        source,
    ))]
    RangeLockFailed {
        blk: u64,
        len: u64,
        source: nix::errno::Errno,
    },
    #[snafu(display(
        "Failed to unlock LBA range for blk {}, len {}, with error: {}",
        blk,
        len,
        source,
    ))]
    RangeUnlockFailed {
        blk: u64,
        len: u64,
        source: nix::errno::Errno,
    },
    #[snafu(display("Failed to get bdev name from URI {}", uri))]
    BdevInvalidUri { source: BdevError, uri: String },
}
