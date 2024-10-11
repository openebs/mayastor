mod bdev_rebuild;
mod nexus_rebuild;
mod rebuild_descriptor;
mod rebuild_error;
mod rebuild_instances;
mod rebuild_job;
mod rebuild_job_backend;
mod rebuild_map;
mod rebuild_state;
mod rebuild_stats;
mod rebuild_task;
mod rebuilders;
mod snapshot_rebuild;

pub use bdev_rebuild::BdevRebuildJob;
pub use nexus_rebuild::{NexusRebuildJob, NexusRebuildJobStarter};
use rebuild_descriptor::RebuildDescriptor;
pub(crate) use rebuild_error::{RebuildError, SnapshotRebuildError};
use rebuild_job::RebuildOperation;
pub use rebuild_job::{RebuildJob, RebuildJobOptions, RebuildVerifyMode};
use rebuild_job_backend::{
    RebuildFBendChan,
    RebuildJobBackendManager,
    RebuildJobRequest,
};
pub use rebuild_map::RebuildMap;
pub use rebuild_state::RebuildState;
use rebuild_state::RebuildStates;
pub(crate) use rebuild_stats::HistoryRecord;
pub use rebuild_stats::RebuildStats;
use rebuild_task::{RebuildTasks, TaskResult};
pub use snapshot_rebuild::SnapshotRebuildJob;

/// Number of concurrent copy tasks per rebuild job
const SEGMENT_TASKS: usize = 16;

/// Size of each segment used by the copy task
pub(crate) const SEGMENT_SIZE: u64 =
    spdk_rs::libspdk::SPDK_BDEV_LARGE_BUF_MAX_SIZE as u64;

/// Checks whether a range is contained within another range
trait WithinRange<T> {
    /// True if `self` is contained within `right`, otherwise false
    fn within(&self, right: std::ops::Range<T>) -> bool;
}

impl WithinRange<u64> for std::ops::Range<u64> {
    fn within(&self, right: std::ops::Range<u64>) -> bool {
        // also make sure ranges don't overflow
        self.start < self.end
            && right.start < right.end
            && self.start >= right.start
            && self.end <= right.end
    }
}

/// Shutdown all pending snapshot rebuilds.
pub(crate) async fn shutdown_snapshot_rebuilds() {
    let jobs = SnapshotRebuildJob::list().into_iter();
    for recv in jobs
        .flat_map(|job| job.force_stop().left())
        .collect::<Vec<_>>()
    {
        recv.await.ok();
    }
}

/// Parse the given url as string into a `url::Url`.
pub fn parse_url(url: &str) -> Result<url::Url, RebuildError> {
    match url::Url::parse(url) {
        Ok(url) => Ok(url),
        Err(source) => Err(RebuildError::BdevInvalidUri {
            source: crate::bdev_api::BdevError::UriParseFailed {
                uri: url.to_owned(),
                source,
            },
            uri: url.to_owned(),
        }),
    }
}
