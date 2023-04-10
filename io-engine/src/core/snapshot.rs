use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Snapshot Captures all the Snapshot information for Lvol.
#[derive(Clone, Debug)]
pub struct SnapshotParams {
    entity_id: Option<String>,
    parent_id: Option<String>,
    txn_id: Option<String>,
    snap_name: Option<String>,
}
/// Implement Snapshot Common Function.
impl SnapshotParams {
    pub fn new(
        entity_id: Option<String>,
        parent_id: Option<String>,
        txn_id: Option<String>,
        snap_name: Option<String>,
    ) -> SnapshotParams {
        SnapshotParams {
            entity_id,
            parent_id,
            txn_id,
            snap_name,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ListSnapshotParams {
    snap_param: SnapshotParams,
    snapshot_uuid: String,
    replica_uuid: String,
    bytes_referenced: u64,
    num_clones: u64,
    timestamp: DateTime<Utc>,
}

impl ListSnapshotParams {
    pub fn new(
        snap_param: SnapshotParams,
        snapshot_uuid: String,
        replica_uuid: String,
        bytes_referenced: u64,
        num_clones: u64,
        timestamp: DateTime<Utc>,
    ) -> ListSnapshotParams {
        ListSnapshotParams {
            snap_param,
            snapshot_uuid,
            replica_uuid,
            bytes_referenced,
            num_clones,
            timestamp,
        }
    }
}
///  Traits gives the common snapshot/clone interface for Local/Remote Lvol.
#[async_trait(?Send)]
pub trait SnapshotOps {
    type Error;
    type SnapshotIter;
    /// Create Snapshot Common API.
    async fn create_snapshot(
        &self,
        snap_param: SnapshotParams,
    ) -> Result<(), Self::Error>;

    /// Get a Snapshot Iterator.
    async fn snapshot_iter(self) -> Self::SnapshotIter;

    /// Prepare Snapshot Config for Block/Nvmf Device, before snapshot create.
    fn prepare_snap_config(
        &self,
        snap_name: &str,
        txn_id: &str,
    ) -> Option<SnapshotParams>;

    /// List Snapshots.
    async fn list_snapshot(self) -> Vec<ListSnapshotParams>;
}

/// Traits gives the Snapshots Related Parameters.
pub trait SnapshotDescriptor {
    /// Get Transaction Id of the Snapshot Create.
    fn txn_id(&self) -> Option<String>;

    /// Get Entity Id of the Snapshot.
    fn entity_id(&self) -> Option<String>;

    /// Get Parent Id of the Snapshot.
    fn parent_id(&self) -> Option<String>;

    /// Get Snapshot Name.
    fn name(&self) -> Option<String>;
}

/// Traits gives ListSnapshot Parameters.
pub trait ListSnapshotParameters {
    /// Get SnapshotParameters.
    fn snapshot_params(&self) -> SnapshotParams;

    /// Get snapshot_uuid.
    fn snapshot_uuid(&self) -> String;

    /// Give replica uuid.
    fn replica_uuid(&self) -> String;

    /// Give Bytes Referenced.
    fn bytes_referenced(&self) -> u64;

    /// Give number of clones.
    fn num_clones(&self) -> u64;

    /// Give timestamp of Snapshot creation.
    fn timestamp(&self) -> DateTime<Utc>;
}

/// Trait to give interface for all Snapshot Parameters.
impl SnapshotDescriptor for SnapshotParams {
    /// Get Trasanction Id of the Snapshot.
    fn txn_id(&self) -> Option<String> {
        self.txn_id.clone()
    }

    /// Get Entity Id of the Snapshot.
    fn entity_id(&self) -> Option<String> {
        self.entity_id.clone()
    }

    /// Get Parent Id of the Snapshot.
    fn parent_id(&self) -> Option<String> {
        self.parent_id.clone()
    }

    /// Get Snapname of the Snapshot.
    fn name(&self) -> Option<String> {
        self.snap_name.clone()
    }
}

impl ListSnapshotParameters for ListSnapshotParams {
    /// Get SnapshotParameters.
    fn snapshot_params(&self) -> SnapshotParams {
        self.snap_param.clone()
    }
    /// Get snapshot_uuid.
    fn snapshot_uuid(&self) -> String {
        self.snapshot_uuid.clone()
    }

    /// Give replica uuid.
    fn replica_uuid(&self) -> String {
        self.replica_uuid.clone()
    }

    /// Give Bytes Referenced.
    fn bytes_referenced(&self) -> u64 {
        self.bytes_referenced
    }

    /// Give number of clones.
    fn num_clones(&self) -> u64 {
        self.num_clones
    }

    /// Give timestamp of Snapshot creation.
    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}
