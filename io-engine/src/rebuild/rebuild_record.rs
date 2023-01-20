use super::{RebuildJob, RebuildState};
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// A rebuild record is a lightweight extract of rebuild job that is maintained
/// for the statistics.
pub struct RebuildRecord {
    /// Source URI of the healthy child to rebuild from.
    pub src_uri: String,
    /// Target URI of the out of sync child in need of a rebuild.
    pub dst_uri: String,
    /// Was this a partial rebuild?
    pub partial_rebuild: bool,
    /// What state this rebuild job ended up in.
    pub state: RebuildState,
    /// Size of rebuilt data: Equal to replica size for full rebuilds,
    /// and lesser(or possibly equal) for partial rebuilds.
    pub rebuilt_data_size: u64,
    /// Start time of this rebuild.
    pub start: DateTime<Utc>,
    /// End time of this rebuild.
    pub end: DateTime<Utc>,
}

impl From<Arc<RebuildJob>> for RebuildRecord {
    fn from(job: Arc<RebuildJob>) -> Self {
        RebuildRecord {
            src_uri: job.src_uri().to_string(),
            dst_uri: job.dst_uri().to_string(),
            // TODO: Set boolean correctly after partial rebuild changes
            partial_rebuild: false,
            state: job.state(),
            // TODO: Set data size correctly after partial rebuild changes
            rebuilt_data_size: 0,
            start: job.start_time(),
            end: Utc::now(),
        }
    }
}
