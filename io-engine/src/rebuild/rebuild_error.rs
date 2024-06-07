use snafu::Snafu;

use crate::{bdev_api::BdevError, core::CoreError};
use spdk_rs::{BdevDescError, DmaError};

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
#[allow(missing_docs)]
/// Various rebuild errors when interacting with a rebuild job or
/// encountered during a rebuild copy
pub enum RebuildError {
    #[snafu(display("Job {} already exists", job))]
    JobAlreadyExists { job: String },
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Source and Destination size range is not compatible"))]
    InvalidSrcDstRange {},
    #[snafu(display("Map range is not compatible with rebuild range"))]
    InvalidMapRange {},
    #[snafu(display(
        "The same device was specified for both source and destination: {bdev}"
    ))]
    SameBdev { bdev: String },
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
    #[snafu(display("Verify IO failed for bdev {}", bdev))]
    VerifyIoFailed { source: CoreError, bdev: String },
    #[snafu(display(
        "Verify compare failed for bdev {}: {}",
        bdev,
        verify_message
    ))]
    VerifyCompareFailed {
        bdev: String,
        verify_message: String,
    },
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
        source: BdevDescError,
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
        source: BdevDescError,
    },
    #[snafu(display("Failed to get bdev name from URI {}", uri))]
    BdevInvalidUri { source: BdevError, uri: String },
    #[snafu(display("The rebuild frontend has been dropped"))]
    FrontendGone,
    #[snafu(display("The rebuild backend has been dropped"))]
    BackendGone,
    #[snafu(display("The rebuild task pool channel is unexpectedly closed with {} active tasks", active))]
    RebuildTasksChannel { active: usize },
    #[snafu(display("Snapshot Rebuild: {source}"))]
    SnapshotRebuild { source: SnapshotRebuildError },
}

/// Various snapshot rebuild errors.
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
#[allow(missing_docs)]
pub enum SnapshotRebuildError {
    #[snafu(display("Local bdev not found"))]
    LocalBdevNotFound {},
    #[snafu(display("Remote bdev uri is missing"))]
    RemoteNoUri {},
    #[snafu(display("Local bdev is not a replica"))]
    NotAReplica {},
    #[snafu(display("Failed to open {uri} as a bdev: {source}"))]
    UriBdevOpen { uri: String, source: BdevError },
}

impl From<SnapshotRebuildError> for RebuildError {
    fn from(source: SnapshotRebuildError) -> Self {
        Self::SnapshotRebuild {
            source,
        }
    }
}
