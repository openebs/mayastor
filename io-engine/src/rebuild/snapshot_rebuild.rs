use std::ops::Deref;

use super::{rebuild_error::RebuildError, RebuildJob, RebuildJobOptions};

use crate::{
    core::SegmentMap,
    gen_rebuild_instances,
    rebuild::{bdev_rebuild::BdevRebuildJobBuilder, BdevRebuildJob},
};

/// A Snapshot rebuild job is responsible for managing a rebuild (copy) which
/// reads from a source snapshot and writes into a local replica from specified
/// start to end.
pub struct SnapshotRebuildJob(BdevRebuildJob);

impl std::fmt::Debug for SnapshotRebuildJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl Deref for SnapshotRebuildJob {
    type Target = RebuildJob;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Builder for the `SnapshotRebuildJob`.
#[derive(Default)]
pub struct SnapshotRebuildJobBuilder(BdevRebuildJobBuilder);
impl SnapshotRebuildJobBuilder {
    /// Specify the rebuild options.
    pub fn with_option(self, options: RebuildJobOptions) -> Self {
        Self(self.0.with_option(options))
    }
    /// Specify a notification function.
    pub fn with_notify_fn(self, notify_fn: fn(&str, &str) -> ()) -> Self {
        Self(self.0.with_notify_fn(notify_fn))
    }
    /// Specify a rebuild map, turning it into a partial rebuild.
    pub fn with_bitmap(self, rebuild_map: SegmentMap) -> Self {
        Self(self.0.with_bitmap(rebuild_map))
    }
    /// Builds a `SnapshotRebuildJob` which can be started and which will then
    /// rebuild from source to destination.
    pub async fn build(
        self,
        src_uri: &str,
        dst_uri: &str,
    ) -> Result<SnapshotRebuildJob, RebuildError> {
        // todo: verify that source is a snapshot, is this possible?
        //  and verify that source is a local replica?
        self.0.build(src_uri, dst_uri).await.map(SnapshotRebuildJob)
    }
}

impl SnapshotRebuildJob {
    /// Helps create a `Self` using a builder: `SnapshotRebuildJobBuilder`.
    pub fn builder() -> SnapshotRebuildJobBuilder {
        SnapshotRebuildJobBuilder::default()
    }
}

gen_rebuild_instances!(SnapshotRebuildJob);
