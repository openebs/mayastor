use super::pool_backend::PoolBackend;
use crate::core::{
    snapshot::SnapshotDescriptor,
    CloneParams,
    LogicalVolume,
    Protocol,
    PtplProps,
    SnapshotParams,
    UpdateProps,
};
use std::fmt::Debug;

/// This interface defines the high level operations which can be done on a
/// `Pool` replica. Replica-Specific details should be hidden away in the
/// implementation as much as possible, though we can allow for extra pool
/// specific options to be passed as parameters.
/// A `Replica` is also a `LogicalVolume` and also has `Share` traits.
#[async_trait::async_trait(?Send)]
pub trait ReplicaOps: LogicalVolume {
    fn shared(&self) -> Option<Protocol>;
    fn create_ptpl(
        &self,
    ) -> Result<Option<PtplProps>, crate::pool_backend::Error>;

    /// Shares the replica via nvmf.
    async fn share_nvmf(
        &mut self,
        props: crate::core::NvmfShareProps,
    ) -> Result<String, crate::pool_backend::Error>;
    /// Unshare the replica.
    async fn unshare(&mut self) -> Result<(), crate::pool_backend::Error>;
    /// Update share properties of a currently shared replica.
    async fn update_properties(
        &mut self,
        props: UpdateProps,
    ) -> Result<(), crate::pool_backend::Error>;

    /// Resize the replica to the given new size.
    async fn resize(
        &mut self,
        size: u64,
    ) -> Result<(), crate::pool_backend::Error>;
    /// Set the replica's entity id.
    async fn set_entity_id(
        &mut self,
        id: String,
    ) -> Result<(), crate::pool_backend::Error>;
    /// Destroy the replica from its parent pool.
    /// # Warning
    /// Destroying implies unsharing, which might fail for some reason, example
    /// if the target is in a bad state, or if IOs are stuck.
    /// todo: return back `Self` in case of an error.
    async fn destroy(self: Box<Self>)
        -> Result<(), crate::pool_backend::Error>;

    /// Snapshot Operations
    ///
    /// Prepare Snapshot Config for Block/Nvmf Device, before snapshot create.
    fn prepare_snap_config(
        &self,
        snap_name: &str,
        entity_id: &str,
        txn_id: &str,
        snap_uuid: &str,
    ) -> Option<SnapshotParams> {
        SnapshotParams::prepare(
            snap_name,
            entity_id,
            txn_id,
            snap_uuid,
            self.uuid(),
        )
    }
    /// Create a snapshot using the given parameters and yields an object which
    /// implements `SnapshotOps`. In turn this can be  used to create clones,
    /// which are `ReplicaOps`.
    async fn create_snapshot(
        &mut self,
        params: SnapshotParams,
    ) -> Result<Box<dyn SnapshotOps>, crate::pool_backend::Error>;
}

/// Snapshot Operations for snapshots created by `ReplicaOps`.
#[async_trait::async_trait(?Send)]
pub trait SnapshotOps: LogicalVolume + Debug {
    /// Destroys the snapshot itself.
    async fn destroy_snapshot(
        self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error>;

    /// Prepares a clone config for creating a clone from a snapshot.
    fn prepare_clone_config(
        &self,
        clone_name: &str,
        clone_uuid: &str,
        source_uuid: &str,
    ) -> Option<CloneParams> {
        CloneParams::prepare(clone_name, clone_uuid, source_uuid)
    }

    /// Creates a **COW** clone from the snapshot.
    /// The cloned object is essentially a replica, and as such it implements
    /// `ReplicaOps`, though it returns its `ReplicaKind` as `Clone`.
    async fn create_clone(
        &self,
        params: CloneParams,
    ) -> Result<Box<dyn ReplicaOps>, crate::pool_backend::Error>;

    /// Gets the `VolumeSnapshotDescriptor` which contains all snapshot related
    /// information.
    /// # Warning: This type is still containing `lvs::Lvol`, which needs to be
    /// refactored out.
    fn descriptor(&self) -> Option<SnapshotDescriptor>;
    /// Check if the snapshot has been discarded.
    /// A snapshot is discarded when it has been deleted but there are still >1
    /// clones which reference its data. In this situation the snapshot may
    /// still exist in the snapshot, but as discarded (and as such unusable).
    fn discarded(&self) -> bool;
}

/// Find replica with filters.
#[derive(Debug, Default)]
pub struct ListReplicaArgs {
    /// Match the given replica name.
    pub name: Option<String>,
    /// Match the given replica uuid.
    pub uuid: Option<String>,
    /// Match the given pool name.
    pub pool_name: Option<String>,
    /// Match the given pool uuid.
    pub pool_uuid: Option<String>,
}

/// Find replica with filters.
#[derive(Debug, Clone)]
pub struct FindReplicaArgs {
    /// The replica uuid to find for.
    pub uuid: String,
}
impl FindReplicaArgs {
    /// Create `Self` with the replica uuid.
    pub fn new(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
        }
    }
}

/// Interface for a replica factory which can be used for various
/// listing operations, for a specific backend type.
#[async_trait::async_trait(?Send)]
pub trait ReplicaFactory {
    /// If the bdev is a `ReplicaOps`, move and retrieve it as a `ReplicaOps`.
    fn bdev_as_replica(
        &self,
        bdev: crate::core::UntypedBdev,
    ) -> Option<Box<dyn ReplicaOps>>;
    /// Finds the replica specified by the arguments, returning None if it
    /// cannot be found.
    async fn find(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, crate::pool_backend::Error>;
    /// Finds the snapshot specified by the arguments, returning None if it
    /// cannot be found.
    async fn find_snap(
        &self,
        args: &FindSnapshotArgs,
    ) -> Result<Option<Box<dyn SnapshotOps>>, crate::pool_backend::Error>;
    /// Lists all replicas specified by the arguments.
    async fn list(
        &self,
        args: &ListReplicaArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error>;
    /// Lists all snapshots specified by the arguments.
    async fn list_snaps(
        &self,
        args: &ListSnapshotArgs,
    ) -> Result<Vec<SnapshotDescriptor>, crate::pool_backend::Error>;
    /// Lists all clones (replicas which have a snapshot parent) specified by
    /// the arguments.
    async fn list_clones(
        &self,
        args: &ListCloneArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error>;
    fn backend(&self) -> PoolBackend;
}

/// Find snapshots with filters.
#[derive(Debug, Default)]
pub struct ListSnapshotArgs {
    /// Match the given snapshot uuid.
    pub uuid: Option<String>,
    /// Match the given source replica uuid.
    pub source_uuid: Option<String>,
}

/// Find replica with filters.
#[derive(Debug, Clone)]
pub struct FindSnapshotArgs {
    /// The snapshot uuid to find for.
    pub uuid: String,
}
impl FindSnapshotArgs {
    /// Create new `Self`.
    pub fn new(uuid: String) -> Self {
        Self {
            uuid,
        }
    }
}

/// List clones with filters.
#[derive(Debug, Default)]
pub struct ListCloneArgs {
    /// Match the given source snapshot uuid.
    pub snapshot_uuid: Option<String>,
}

/// Get the `ReplicaFactory` for the given backend type.
pub(crate) fn factory_enabled(
    backend: PoolBackend,
) -> Option<Box<dyn ReplicaFactory>> {
    backend.enabled().ok()?;
    Some(factory_unsafe(backend))
}
/// Get the `ReplicaFactory` for the given backend type.
pub(crate) fn factory_unsafe(backend: PoolBackend) -> Box<dyn ReplicaFactory> {
    match backend {
        PoolBackend::Lvs => Box::new(crate::lvs::ReplLvsFactory {}) as _,
        PoolBackend::Lvm => Box::new(crate::lvm::ReplLvmFactory {}) as _,
    }
}
/// Get all the enabled `ReplicaFactory`.
pub(crate) fn factories() -> Vec<Box<dyn ReplicaFactory>> {
    vec![PoolBackend::Lvm, PoolBackend::Lvs]
        .into_iter()
        .filter_map(factory_enabled)
        .collect()
}
/// Get the given bdev as a `ReplicaOps`.
pub(crate) fn bdev_as_replica(
    bdev: crate::core::UntypedBdev,
) -> Option<Box<dyn ReplicaOps>> {
    for factory in factories() {
        if let Some(replica) = factory.bdev_as_replica(bdev) {
            return Some(replica);
        }
    }
    None
}
