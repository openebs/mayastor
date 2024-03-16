use snafu::ResultExt;
use std::{convert::TryFrom, ops::Deref};

use super::{rebuild_error::RebuildError, RebuildJob, RebuildJobOptions};

use crate::{
    bdev::{device_create, device_destroy},
    core::{Bdev, Reactors, SegmentMap},
    gen_rebuild_instances,
    lvs::Lvol,
    rebuild::{
        bdev_rebuild::BdevRebuildJobBuilder,
        rebuild_error::{SnapshotRebuildError, SourceUriBdev},
        BdevRebuildJob,
    },
};

/// A Snapshot rebuild job is responsible for managing a rebuild (copy) which
/// reads from a source snapshot and writes into a local replica from specified
/// start to end.
pub struct SnapshotRebuildJob {
    inner: BdevRebuildJob,
    replica_uuid: String,
}

impl std::fmt::Debug for SnapshotRebuildJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
impl Deref for SnapshotRebuildJob {
    type Target = RebuildJob;

    fn deref(&self) -> &Self::Target {
        &self.inner
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
        replica_uuid: &str,
    ) -> Result<SnapshotRebuildJob, RebuildError> {
        // todo: verify that source is a snapshot, is this even possible?

        // ensure that replica exists
        // todo: when we have new backends, we can't just use `Lvol` directly.
        let _lvol = Bdev::lookup_by_uuid_str(replica_uuid)
            .ok_or(SnapshotRebuildError::ReplicaBdevNotFound {})
            .and_then(|bdev| {
                Lvol::try_from(bdev)
                    .map_err(|_| SnapshotRebuildError::NotAReplica {})
            })?;

        device_create(src_uri).await.context(SourceUriBdev)?;

        let dst_uri = format!("bdev:///{replica_uuid}");
        match self.0.build(src_uri, &dst_uri).await {
            Ok(job) => Ok(SnapshotRebuildJob::new(replica_uuid, job)),
            Err(error) => {
                device_destroy(src_uri).await.ok();
                Err(error)
            }
        }
    }
}

impl SnapshotRebuildJob {
    /// Helps create a `Self` using a builder: `SnapshotRebuildJobBuilder`.
    pub fn builder() -> SnapshotRebuildJobBuilder {
        SnapshotRebuildJobBuilder::default()
    }
    fn new(replica_uuid: &str, job: BdevRebuildJob) -> Self {
        Self {
            replica_uuid: replica_uuid.to_owned(),
            inner: job,
        }
    }
    pub fn list() -> Vec<std::sync::Arc<SnapshotRebuildJob>> {
        Self::get_instances().values().cloned().collect()
    }
    pub fn name(&self) -> &str {
        &self.replica_uuid
    }
    pub fn destroy(self: std::sync::Arc<Self>) {
        let _ = Self::remove(self.name());
    }
}

impl Drop for SnapshotRebuildJob {
    fn drop(&mut self) {
        let src_uri = self.src_uri().to_owned();
        Reactors::master().send_future(async move {
            if let Err(error) = device_destroy(&src_uri).await {
                // todo: how do we know it's safe to destroy?
                //  we don't use refcounts for this, but maybe we should?
                tracing::error!(
                    "Failed to close source of rebuild job: {error}"
                );
            }
        });
    }
}

gen_rebuild_instances!(SnapshotRebuildJob);
