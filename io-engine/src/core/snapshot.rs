use crate::lvs::Lvol;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strum_macros::{EnumCount as EnumCountMacro, EnumIter};

/// Snapshot Captures all the Snapshot information for Lvol.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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

/// VolumeSnapshot Parameters to list the snapshots.
#[derive(Clone, Debug)]
pub struct VolumeSnapshotDescriptor {
    snap_param: SnapshotParams,
    snapshot_uuid: String,
    replica_uuid: String,
    bytes_referenced: u64,
    num_clones: u64,
    timestamp: DateTime<Utc>,
}

impl VolumeSnapshotDescriptor {
    pub fn new(
        snap_param: SnapshotParams,
        snapshot_uuid: String,
        replica_uuid: String,
        bytes_referenced: u64,
        num_clones: u64,
        timestamp: DateTime<Utc>,
    ) -> VolumeSnapshotDescriptor {
        VolumeSnapshotDescriptor {
            snap_param,
            snapshot_uuid,
            replica_uuid,
            bytes_referenced,
            num_clones,
            timestamp,
        }
    }
}
/// Snapshot attributes used to store its properties.
#[derive(Debug, EnumCountMacro, EnumIter)]
pub enum SnapshotXattrs {
    TxId,
    EntityId,
    ParentId,
}

impl SnapshotXattrs {
    pub fn name(&self) -> &'static str {
        match *self {
            Self::TxId => "mayastor.tx_id",
            Self::EntityId => "mayastor.entity_id",
            Self::ParentId => "mayastor.parent_id",
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
    ) -> Result<Option<Lvol>, Self::Error>;

    // Get a Snapshot Iterator.
    async fn snapshot_iter(self) -> Self::SnapshotIter;

    /// Prepare Snapshot Config for Block/Nvmf Device, before snapshot create.
    fn prepare_snap_config(
        &self,
        snap_name: &str,
        txn_id: &str,
    ) -> Option<SnapshotParams>;

    /// List Snapshots.
    fn list_snapshot(self) -> Vec<VolumeSnapshotDescriptor>;
}

/// Traits gives the Snapshots Related Parameters.
pub trait SnapshotDescriptor {
    /// Get Transaction Id of the Snapshot Create.
    fn txn_id(&self) -> Option<String>;

    /// Set Transaction Id.
    fn set_txn_id(&mut self, txn_id: String);

    /// Get Entity Id of the Snapshot.
    fn entity_id(&self) -> Option<String>;

    /// Set Entity Id.
    fn set_entity_id(&mut self, entity_id: String);

    /// Get Parent Id of the Snapshot.
    fn parent_id(&self) -> Option<String>;

    /// Set Parent id of the Snapshot.
    fn set_parent_id(&mut self, parent_id: String);

    /// Get Snapshot Name.
    fn name(&self) -> Option<String>;

    /// Set Snapshot Name.
    fn set_name(&mut self, name: String);
}

/// Traits gives VolumeSnapshot Descriptor.
pub trait VolumeSnapshotDescriptors {
    /// Get SnapshotParameters.
    fn snapshot_params(&self) -> &SnapshotParams;

    /// Get snapshot_uuid.
    fn snapshot_uuid(&self) -> &str;

    /// Give replica uuid.
    fn replica_uuid(&self) -> &str;

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

    /// Set Transaction Id.
    fn set_txn_id(&mut self, txn_id: String) {
        self.txn_id = Some(txn_id);
    }
    /// Get Entity Id of the Snapshot.
    fn entity_id(&self) -> Option<String> {
        self.entity_id.clone()
    }

    /// Set Entity Id.
    fn set_entity_id(&mut self, entity_id: String) {
        self.entity_id = Some(entity_id);
    }

    /// Get Parent Id of the Snapshot.
    fn parent_id(&self) -> Option<String> {
        self.parent_id.clone()
    }

    /// Set Parent id of the Snapshot.
    fn set_parent_id(&mut self, parent_id: String) {
        self.parent_id = Some(parent_id)
    }
    /// Get Snapname of the Snapshot.
    fn name(&self) -> Option<String> {
        self.snap_name.clone()
    }
    /// Set Snapshot Name.
    fn set_name(&mut self, name: String) {
        self.snap_name = Some(name);
    }
}

impl VolumeSnapshotDescriptors for VolumeSnapshotDescriptor {
    /// Get SnapshotParameters.
    fn snapshot_params(&self) -> &SnapshotParams {
        &self.snap_param
    }
    /// Get snapshot_uuid.
    fn snapshot_uuid(&self) -> &str {
        &self.snapshot_uuid
    }

    /// Give replica uuid.
    fn replica_uuid(&self) -> &str {
        &self.replica_uuid
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
