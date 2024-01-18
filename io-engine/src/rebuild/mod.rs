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

pub use bdev_rebuild::BdevRebuildJob;
pub use nexus_rebuild::NexusRebuildJob;
use rebuild_descriptor::RebuildDescriptor;
pub(crate) use rebuild_error::RebuildError;
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
