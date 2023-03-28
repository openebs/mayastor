use crate::core::logical_volume::LogicalVolume;
use async_trait::async_trait;

/// Snapshot Captures all the Snapshot information for Lvol.
#[derive(Debug, Default)]
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

    // Get a Snapshot Iterator.
    async fn snapshot_iter(self) -> Self::SnapshotIter;
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

/// Parameters for a clone of snapshot.
pub struct CloneParams {}

/// Descriptor of a newly created clone.
pub struct CloneDescriptor {}

/// Snapshot specific operations.
#[async_trait(?Send)]
pub trait Snapshot: LogicalVolume + SnapshotDescriptor {
    async fn clone(params: CloneParams) -> Result<CloneDescriptor, String>;
}
