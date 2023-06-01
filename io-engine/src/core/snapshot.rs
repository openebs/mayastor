use crate::{core::logical_volume::LogicalVolume, lvs::Lvol};
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
    snapshot_uuid: Option<String>,
}

/// Implement Snapshot Common Function.
impl SnapshotParams {
    pub fn new(
        entity_id: Option<String>,
        parent_id: Option<String>,
        txn_id: Option<String>,
        snap_name: Option<String>,
        snapshot_uuid: Option<String>,
    ) -> SnapshotParams {
        SnapshotParams {
            entity_id,
            parent_id,
            txn_id,
            snap_name,
            snapshot_uuid,
        }
    }
}

/// Snapshot Descriptor to respond back as part of listsnapshot
#[derive(Clone, Debug)]
pub struct VolumeSnapshotDescriptor {
    snapshot_uuid: String,
    snapshot_size: u64,
    num_clones: u64,
    timestamp: DateTime<Utc>,
    replica_uuid: String,
    replica_size: u64,
    snap_param: SnapshotParams,
    // set to false, if any of the snapshotdescriptor is not filled properly
    valid_snapshot: bool,
}

impl VolumeSnapshotDescriptor {
    pub fn new(
        snapshot: &Lvol,
        num_clones: u64,
        timestamp: DateTime<Utc>,
        replica_uuid: String,
        replica_size: u64,
        snap_param: SnapshotParams,
        valid_snapshot: bool,
    ) -> Self {
        Self {
            snapshot_uuid: snapshot.uuid(),
            snapshot_size: snapshot.size(),
            num_clones,
            timestamp,
            replica_uuid,
            replica_size,
            snap_param,
            valid_snapshot,
        }
    }
}
/// Snapshot attributes used to store its properties.
#[derive(Debug, EnumCountMacro, EnumIter)]
pub enum SnapshotXattrs {
    TxId,
    EntityId,
    ParentId,
    SnapshotUuid,
}

impl SnapshotXattrs {
    pub fn name(&self) -> &'static str {
        match *self {
            Self::TxId => "io-engine.tx_id",
            Self::EntityId => "io-engine.entity_id",
            Self::ParentId => "io-engine.parent_id",
            Self::SnapshotUuid => "uuid",
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
    ) -> Result<Lvol, Self::Error>;

    // Get a Snapshot Iterator.
    async fn snapshot_iter(self) -> Self::SnapshotIter;

    /// Prepare Snapshot Config for Block/Nvmf Device, before snapshot create.
    fn prepare_snap_config(
        &self,
        snap_name: &str,
        entity_id: &str,
        txn_id: &str,
        snap_uuid: &str,
    ) -> Option<SnapshotParams>;

    /// List Snapshots.
    fn list_snapshot(&self) -> Vec<VolumeSnapshotDescriptor>;
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

    /// Get snapshot uuid of the snapshot.
    fn snapshot_uuid(&self) -> Option<String>;

    /// Set snapshot uuid of the snapshot.
    fn set_snapshot_uuid(&mut self, snapshot_uuid: String);
}

/// Traits gives VolumeSnapshot Descriptor.
pub trait VolumeSnapshotDescriptors {
    /// Get snapshot_uuid.
    fn snapshot_uuid(&self) -> &str;

    /// Get snapshot size.
    fn snapshot_size(&self) -> u64;

    /// Give number of clones.
    fn num_clones(&self) -> u64;

    /// Give timestamp of Snapshot creation.
    fn timestamp(&self) -> DateTime<Utc>;

    /// Give replica uuid.
    fn replica_uuid(&self) -> &str;

    /// Give replica size.
    fn replica_size(&self) -> u64;

    /// Get SnapshotParameters.
    fn snapshot_params(&self) -> &SnapshotParams;

    /// Get ValidSnapshot value.
    fn valid_snapshot(&self) -> bool;
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
    /// Get snapshot uuid of the snapshot.
    fn snapshot_uuid(&self) -> Option<String> {
        self.snapshot_uuid.clone()
    }
    /// Set snapshot uuid of the snapshot.
    fn set_snapshot_uuid(&mut self, snapshot_uuid: String) {
        self.snapshot_uuid = Some(snapshot_uuid);
    }
}

impl VolumeSnapshotDescriptors for VolumeSnapshotDescriptor {
    /// Get snapshot_uuid.
    fn snapshot_uuid(&self) -> &str {
        &self.snapshot_uuid
    }

    /// Give snapshot size.
    fn snapshot_size(&self) -> u64 {
        self.snapshot_size
    }

    /// Give number of clones.
    fn num_clones(&self) -> u64 {
        self.num_clones
    }

    /// Give timestamp of Snapshot creation.
    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Give replica uuid.
    fn replica_uuid(&self) -> &str {
        &self.replica_uuid
    }

    /// Give replica size.
    fn replica_size(&self) -> u64 {
        self.replica_size
    }

    /// Get SnapshotParameters.
    fn snapshot_params(&self) -> &SnapshotParams {
        &self.snap_param
    }

    /// Get ValidSnapshot value.
    fn valid_snapshot(&self) -> bool {
        self.valid_snapshot
    }
}
