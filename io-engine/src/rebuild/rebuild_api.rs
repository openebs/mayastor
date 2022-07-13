#![warn(missing_docs)]

use std::fmt;

use crossbeam::channel::{Receiver, Sender};
use futures::channel::oneshot;
use snafu::Snafu;

use crate::{
    bdev::{nexus::VerboseError, Nexus},
    core::{BlockDeviceDescriptor, CoreError, DescriptorGuard},
    nexus_uri::NexusBdevError,
};
use spdk_rs::DmaError;

use super::rebuild_impl::*;

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility = "pub(crate)")]
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
    IoError { source: CoreError, bdev: String },
    #[snafu(display("Read IO failed for bdev {}", bdev))]
    ReadIoError { source: CoreError, bdev: String },
    #[snafu(display("Write IO failed for bdev {}", bdev))]
    WriteIoError { source: CoreError, bdev: String },
    #[snafu(display("Failed to find rebuild job {}", job))]
    JobNotFound { job: String },
    #[snafu(display("Job {} already exists", job))]
    JobAlreadyExists { job: String },
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
    RangeLockError {
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
    RangeUnLockError {
        blk: u64,
        len: u64,
        source: nix::errno::Errno,
    },
    #[snafu(display("Failed to get bdev name from URI {}", uri))]
    BdevInvalidUri { source: NexusBdevError, uri: String },
}

#[derive(Debug, PartialEq, Copy, Clone)]
/// allowed states for a rebuild job
pub enum RebuildState {
    /// Init when the job is newly created
    Init,
    /// Running when the job is rebuilding
    Running,
    /// Stopped when the job is halted as requested through stop
    /// and pending its removal
    Stopped,
    /// Paused when the job is paused as requested through pause
    Paused,
    /// Failed when an IO (R/W) operation was failed
    /// there are no retries as it currently stands
    Failed,
    /// Completed when the rebuild was successfully completed
    Completed,
}

impl fmt::Display for RebuildState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RebuildState::Init => write!(f, "init"),
            RebuildState::Running => write!(f, "running"),
            RebuildState::Stopped => write!(f, "stopped"),
            RebuildState::Paused => write!(f, "paused"),
            RebuildState::Failed => write!(f, "failed"),
            RebuildState::Completed => write!(f, "completed"),
        }
    }
}

impl RebuildState {
    /// Final update for a rebuild job
    pub fn done(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Completed)
    }
}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end
pub struct RebuildJob<'n> {
    /// name of the nexus associated with the rebuild job
    pub nexus_name: String,
    /// descriptor for the nexus
    pub(super) nexus_descriptor: DescriptorGuard<Nexus<'n>>,
    /// source URI of the healthy child to rebuild from
    pub src_uri: String,
    /// target URI of the out of sync child in need of a rebuild
    pub dst_uri: String,
    pub(super) block_size: u64,
    pub(super) range: std::ops::Range<u64>,
    pub(super) next: u64,
    pub(super) segment_size_blks: u64,
    pub(super) task_pool: RebuildTasks,
    pub(super) notify_fn: fn(String, String) -> (),
    /// channel used to signal rebuild update
    pub notify_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    /// current state of the rebuild job
    pub(super) states: RebuildStates,
    /// channel list which allows the await of the rebuild
    pub(super) complete_chan: Vec<oneshot::Sender<RebuildState>>,
    /// rebuild copy error, if any
    pub error: Option<RebuildError>,

    // Pre-opened descriptors for source/destination block device.
    pub(super) src_descriptor: Box<dyn BlockDeviceDescriptor>,
    pub(super) dst_descriptor: Box<dyn BlockDeviceDescriptor>,
}

// TODO: is `RebuildJob` really a Send type?
unsafe impl Send for RebuildJob<'_> {}

impl<'n> fmt::Debug for RebuildJob<'n> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RebuildJob")
            .field("nexus", &self.nexus_name)
            .field("source", &self.src_uri)
            .field("destination", &self.dst_uri)
            .finish()
    }
}

/// rebuild statistics
pub struct RebuildStats {
    /// total number of blocks to recover
    pub blocks_total: u64,
    /// number of blocks recovered
    pub blocks_recovered: u64,
    /// rebuild progress in %
    pub progress: u64,
    /// granularity of each recovery copy in blocks
    pub segment_size_blks: u64,
    /// size in bytes of each block
    pub block_size: u64,
    /// total number of concurrent rebuild tasks
    pub tasks_total: u64,
    /// number of current active tasks
    pub tasks_active: u64,
}

impl<'n> RebuildJob<'n> {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments
    pub fn create<'a>(
        nexus_name: &str,
        src_uri: &str,
        dst_uri: &'a str,
        range: std::ops::Range<u64>,
        notify_fn: fn(String, String) -> (),
    ) -> Result<&'a mut Self, RebuildError> {
        Self::new(nexus_name, src_uri, dst_uri, range, notify_fn)?.store()?;

        Self::lookup(dst_uri)
    }

    /// Lookup a rebuild job by its destination uri and return it
    pub fn lookup<'a>(
        dst_uri: &str,
    ) -> Result<&'a mut RebuildJob<'a>, RebuildError> {
        if let Some(job) = Self::get_instances().get_mut(dst_uri) {
            Ok(Self::from_static(job))
        } else {
            Err(RebuildError::JobNotFound {
                job: dst_uri.to_owned(),
            })
        }
    }

    /// Lookup all rebuilds jobs with name as its source.
    pub fn lookup_src<'a>(src_uri: &str) -> Vec<&'a mut RebuildJob<'a>> {
        Self::get_instances()
            .iter_mut()
            .filter_map(|j| {
                if j.1.src_uri == src_uri {
                    Some(Self::from_static(j.1.as_mut()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Lookup a rebuild job by its destination uri then remove and drop it.
    pub fn remove(name: &str) -> Result<(), RebuildError> {
        match Self::get_instances().remove(name) {
            Some(_) => Ok(()),
            None => Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            }),
        }
    }

    /// Number of rebuild job instances
    pub fn count() -> usize {
        Self::get_instances().len()
    }

    /// State of the rebuild job
    pub fn state(&self) -> RebuildState {
        self.states.current
    }

    /// Error description
    pub fn error_desc(&self) -> String {
        match self.error.as_ref() {
            Some(e) => e.verbose(),
            _ => "".to_string(),
        }
    }
}
