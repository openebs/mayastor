use super::RebuildState;
use chrono::{DateTime, Utc};
use std::ops::Deref;

/// Rebuild statistics.
#[derive(Debug, Clone)]
pub struct RebuildStats {
    /// Total number of blocks to recover.
    pub blocks_total: u64,
    /// Number of blocks recovered.
    pub blocks_recovered: u64,
    /// Number of blocks for which the actual data transfer occurred.
    pub blocks_transferred: u64,
    /// Number of blocks remaining to transfer.
    pub blocks_remaining: u64,
    /// Rebuild progress in %.
    pub progress: u64,
    /// Granularity of each recovery copy in blocks.
    pub blocks_per_task: u64,
    /// Size in bytes of each block.
    pub block_size: u64,
    /// Total number of concurrent rebuild tasks.
    pub tasks_total: u64,
    /// Number of current active tasks.
    pub tasks_active: u64,
    /// Start time of this rebuild.
    pub start_time: DateTime<Utc>,
    /// Is this a partial rebuild?
    pub is_partial: bool,
}

impl Default for RebuildStats {
    fn default() -> Self {
        Self {
            blocks_total: 0,
            blocks_recovered: 0,
            blocks_transferred: 0,
            blocks_remaining: 0,
            progress: 0,
            blocks_per_task: 0,
            block_size: 0,
            tasks_total: 0,
            tasks_active: 0,
            start_time: Utc::now(),
            is_partial: false,
        }
    }
}

/// A rebuild record is a lightweight extract of rebuild job that is maintained
/// for the statistics.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HistoryRecord {
    /// Target URI of the out of sync child in need of a rebuild.
    pub child_uri: String,
    /// Source URI of the healthy child to rebuild from.
    pub src_uri: String,
    /// Final stats collected after the rebuild finished.
    pub(super) final_stats: RebuildStats,
    /// What state this rebuild job ended up in.
    pub state: RebuildState,
    /// End time of this rebuild.
    pub end_time: DateTime<Utc>,
}

impl Deref for HistoryRecord {
    type Target = RebuildStats;

    fn deref(&self) -> &Self::Target {
        &self.final_stats
    }
}
