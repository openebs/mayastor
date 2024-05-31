use std::{convert::TryFrom, ops::Deref, sync::Arc};

use super::{rebuild_error::RebuildError, RebuildJob, RebuildJobOptions};

use crate::{
    bdev::{device_create, device_destroy},
    bdev_api::BdevError,
    core::{Bdev, LogicalVolume, Reactors, ReadOptions, SegmentMap},
    gen_rebuild_instances,
    rebuild::{
        bdev_rebuild::BdevRebuildJobBuilder,
        rebuild_error::SnapshotRebuildError,
        BdevRebuildJob,
    },
};

/// A Snapshot rebuild job is responsible for managing a rebuild (copy) which
/// reads from a source snapshot and writes into a local replica from specified
/// start to end.
pub struct SnapshotRebuildJob {
    inner: BdevRebuildJob,
    uuid: String,
    replica_uuid: String,
    snapshot_uuid: String,
    replica_uri: Uri,
    snapshot_uri: Uri,
}

impl std::fmt::Debug for SnapshotRebuildJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotRebuildJob")
            .field("uuid", &self.uuid)
            .field("inner", &self.inner)
            .finish()
    }
}
impl Deref for SnapshotRebuildJob {
    type Target = RebuildJob;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Default)]
struct Uri {
    create: bool,
    delete: bool,
    uri: String,
}
impl Uri {
    /// Return a new `Self` with flag indicating if it needs to be created
    /// before it can be opened.
    pub fn new<I: Into<String>>(uri: I, create: bool) -> Self {
        Self {
            create,
            delete: false,
            uri: uri.into(),
        }
    }
    /// Opens or Creates the uri.
    /// If created then the uri is also closed on drop.
    async fn open_create(mut self) -> Result<Uri, SnapshotRebuildError> {
        if self.create {
            if let Err(source) = device_create(&self.uri).await {
                if matches!(source, BdevError::BdevExists { .. }) {
                    self.delete = false;
                } else {
                    return Err(SnapshotRebuildError::UriBdevOpen {
                        uri: self.uri.clone(),
                        source,
                    });
                }
            } else {
                self.delete = true;
            }
        }
        Ok(self)
    }
    /// Closes the uri if it was created by this.
    async fn close(mut self) -> Result<(), SnapshotRebuildError> {
        if self.delete {
            Self::destroy(&self.uri).await;
            self.delete = false;
        }
        Ok(())
    }
    /// Destroys the uri device.
    /// # Warning: destruction is a fallible process!
    /// In case of failure not much we can do, other than logging it.
    async fn destroy(uri: &str) {
        if let Err(error) = device_destroy(uri).await {
            // todo: how do we know it's safe to destroy?
            //  we don't use refcounts for this, but maybe we should?
            tracing::error!("Failed to destroy uri {uri}: {error}");
        }
    }
}
impl Drop for Uri {
    fn drop(&mut self) {
        if !self.delete {
            return;
        }
        let uri = self.uri.clone();
        // destruction is async so we cannot rely on its destruction by
        // the time drop ends :(
        Reactors::master().send_future(async move { Uri::destroy(&uri).await });
    }
}

/// Builder for the `SnapshotRebuildJob`.
#[derive(Default)]
pub struct SnapshotRebuildJobBuilder {
    bdev_builder: BdevRebuildJobBuilder,
    uuid: Option<String>,
    snapshot_uri: String,
    replica_uri: String,
    replica_uuid: String,
    snapshot_uuid: String,
}
impl SnapshotRebuildJobBuilder {
    fn builder() -> Self {
        Default::default()
    }
    /// Specify the rebuild options.
    pub fn with_option(mut self, options: RebuildJobOptions) -> Self {
        self.bdev_builder = self.bdev_builder.with_option(options);
        self
    }
    /// Specify a notification function.
    pub fn with_notify_fn(mut self, notify_fn: fn(&str, &str) -> ()) -> Self {
        self.bdev_builder = self.bdev_builder.with_notify_fn(notify_fn);
        self
    }
    /// Specify a rebuild map, turning it into a partial rebuild.
    pub fn with_bitmap(mut self, rebuild_map: SegmentMap) -> Self {
        self.bdev_builder = self.bdev_builder.with_bitmap(rebuild_map);
        self
    }
    /// Specify the snapshot uuid.
    pub fn with_snapshot_uuid(mut self, uuid: &str) -> Self {
        self.snapshot_uuid = uuid.to_string();
        self
    }
    /// Specify the replica uuid.
    pub fn with_replica_uuid(mut self, uuid: &str) -> Self {
        self.replica_uuid = uuid.to_string();
        if self.uuid.is_none() {
            self.uuid = Some(uuid.to_string());
        }
        self
    }
    /// Specify the job's uuid.
    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_string());
        self
    }
    /// Specify the replica uri.
    pub fn with_replica_uri<S: Into<String>>(mut self, uri: S) -> Self {
        self.replica_uri = uri.into();
        self
    }
    /// Specify the snapshot uri.
    pub fn with_snapshot_uri<S: Into<String>>(mut self, uri: S) -> Self {
        self.snapshot_uri = uri.into();
        self
    }
    // todo: we have new backends, we shouldn't just use `Lvol` directly.
    fn lookup_lvol(uri: &str) -> Result<crate::lvs::Lvol, RebuildError> {
        let lvol = Bdev::lookup_by_uuid_str(uri)
            .ok_or(SnapshotRebuildError::LocalBdevNotFound {})
            .and_then(|bdev| {
                crate::lvs::Lvol::try_from(bdev)
                    .map_err(|_| SnapshotRebuildError::NotAReplica {})
            })?;
        Ok(lvol)
    }
    fn snapshot_uri(&self) -> Result<Uri, RebuildError> {
        if !self.snapshot_uri.is_empty() {
            return Ok(Uri::new(&self.snapshot_uri, true));
        }
        let lvol = Self::lookup_lvol(&self.snapshot_uuid)?;
        if !lvol.is_snapshot() {
            // Not a snapshot, fail?
        }

        Ok(Uri::new(format!("bdev:///{}", self.snapshot_uuid), false))
    }
    fn replica_uri(&self) -> Result<Uri, RebuildError> {
        if !self.replica_uri.is_empty() {
            return Ok(Uri::new(&self.replica_uri, true));
        }

        let lvol = Self::lookup_lvol(&self.replica_uuid)?;
        if lvol.is_snapshot() {
            // Not a replica, fail?
        }

        Ok(Uri::new(format!("bdev:///{}", self.replica_uuid), false))
    }
    async fn build_uris(&self) -> Result<(Uri, Uri), RebuildError> {
        let snapshot_uri = self.snapshot_uri()?;
        let replica_uri = self.replica_uri()?;

        let snapshot = snapshot_uri.open_create().await?;
        let replica = match replica_uri.open_create().await {
            Ok(uri) => uri,
            Err(error) => {
                snapshot.close().await.ok();
                return Err(error.into());
            }
        };

        Ok((snapshot, replica))
    }

    /// Builds a `SnapshotRebuildJob` which can be started and which will then
    /// rebuild from snapshot uri to replica uri.
    pub async fn build(self) -> Result<SnapshotRebuildJob, RebuildError> {
        let (snapshot_uri, replica_uri) = self.build_uris().await?;

        match self
            .bdev_builder
            .build(&snapshot_uri.uri, &replica_uri.uri)
            .await
        {
            Ok(inner) => Ok(SnapshotRebuildJob {
                inner,
                uuid: self
                    .uuid
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                replica_uuid: self.replica_uuid,
                snapshot_uuid: self.snapshot_uuid,
                snapshot_uri,
                replica_uri,
            }),
            Err(error) => {
                snapshot_uri.close().await.ok();
                replica_uri.close().await.ok();
                Err(error)
            }
        }
    }
}

impl SnapshotRebuildJob {
    /// Helps create a `Self` using a builder: `SnapshotRebuildJobBuilder`.
    pub fn builder() -> SnapshotRebuildJobBuilder {
        SnapshotRebuildJobBuilder::builder().with_option(
            RebuildJobOptions::default()
                .with_read_opts(ReadOptions::CurrentUnwrittenFail),
        )
    }
    /// Get a list of all snapshot rebuild jobs.
    pub fn list() -> Vec<Arc<SnapshotRebuildJob>> {
        Self::get_instances().values().cloned().collect()
    }
    /// Get the name of this rebuild job.
    pub fn name(&self) -> &str {
        self.uuid()
    }
    /// Get the uuid of this rebuild job.
    pub fn uuid(&self) -> &str {
        &self.uuid
    }
    /// Get the replica uri.
    pub fn replica_uri(&self) -> &str {
        &self.replica_uri.uri
    }
    /// Get the snapshot uri.
    pub fn snapshot_uri(&self) -> &str {
        &self.snapshot_uri.uri
    }
    /// Get the replica uuid.
    pub fn replica_uuid(&self) -> &str {
        &self.replica_uuid
    }
    /// Get the snapshot uuid.
    pub fn snapshot_uuid(&self) -> &str {
        &self.snapshot_uuid
    }
    /// Destroy this snapshot rebuild job itself.
    pub fn destroy(self: Arc<Self>) {
        let _ = Self::remove(self.uuid());
    }
}

gen_rebuild_instances!(SnapshotRebuildJob);
