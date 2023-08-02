use super::{compose::rpc::v1::SharedRpcHandle, generate_uuid};

use mayastor_api::v1::snapshot::{
    CreateReplicaSnapshotRequest,
    CreateReplicaSnapshotResponse,
    CreateSnapshotCloneRequest,
    ListSnapshotCloneRequest,
    // ListSnapshotCloneResponse,
    ListSnapshotsRequest,
    // ListSnapshotsResponse,
    Replica,
    SnapshotInfo,
    SnapshotQueryType,
};
use tonic::Status;

pub struct ReplicaSnapshotBuilder {
    pub rpc: SharedRpcHandle,
    pub replica_uuid: Option<String>,
    pub snapshot_uuid: Option<String>,
    pub snapshot_name: Option<String>,
    pub entity_id: Option<String>,
    pub txn_id: Option<String>,
}
impl ReplicaSnapshotBuilder {
    pub fn new(rpc: SharedRpcHandle) -> Self {
        Self {
            rpc,
            replica_uuid: None,
            snapshot_uuid: None,
            snapshot_name: None,
            entity_id: None,
            txn_id: None,
        }
    }
    pub fn with_replica_uuid(mut self, replica_uuid: &str) -> Self {
        self.replica_uuid = Some(replica_uuid.to_owned());
        self
    }
    pub fn with_snapshot_uuid(mut self) -> Self {
        self.snapshot_uuid = Some(generate_uuid());
        self
    }
    pub fn with_snapshot_name(mut self, snap_name: &str) -> Self {
        self.snapshot_name = Some(snap_name.to_owned());
        self
    }
    pub fn with_entity_id(mut self, entity_id: &str) -> Self {
        self.entity_id = Some(entity_id.to_owned());
        self
    }
    pub fn with_txn_id(mut self, txn_id: &str) -> Self {
        self.txn_id = Some(txn_id.to_owned());
        self
    }
    pub fn snapshot_uuid(&self) -> String {
        self.snapshot_uuid
            .as_ref()
            .expect("Snapshot UUID must be set")
            .clone()
    }
    pub fn replica_uuid(&self) -> String {
        self.replica_uuid
            .as_ref()
            .expect("Replica UUID must be set")
            .clone()
    }
    pub fn snapshot_name(&self) -> String {
        self.snapshot_name
            .as_ref()
            .expect("Snapshot name must be set")
            .clone()
    }
    pub fn rpc(&self) -> SharedRpcHandle {
        self.rpc.clone()
    }
    pub async fn create_replica_snapshot(
        &mut self,
    ) -> Result<CreateReplicaSnapshotResponse, Status> {
        self.rpc()
            .lock()
            .await
            .snapshot
            .create_replica_snapshot(CreateReplicaSnapshotRequest {
                replica_uuid: self.replica_uuid(),
                snapshot_uuid: self.snapshot_uuid(),
                snapshot_name: self.snapshot_name(),
                entity_id: self.entity_id.as_ref().unwrap().to_string(),
                txn_id: self.txn_id.as_ref().unwrap().to_string(),
            })
            .await
            .map(|r| r.into_inner())
    }
    pub async fn get_snapshots(&self) -> Result<Vec<SnapshotInfo>, Status> {
        Ok(list_snapshot(self.rpc())
            .await
            .expect("List Snapshot Failed")
            .into_iter()
            .filter(|s| s.source_uuid == self.replica_uuid())
            .collect::<Vec<_>>())
    }
}
pub async fn list_snapshot(
    rpc: SharedRpcHandle,
) -> Result<Vec<SnapshotInfo>, Status> {
    rpc.lock()
        .await
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            snapshot_query_type: SnapshotQueryType::AllSnapshots as i32,
        })
        .await
        .map(|r| r.into_inner().snapshots)
}

pub struct SnapshotCloneBuilder {
    pub rpc: SharedRpcHandle,
    pub snapshot_uuid: Option<String>,
    pub clone_name: Option<String>,
    pub clone_uuid: Option<String>,
}
impl SnapshotCloneBuilder {
    pub fn new(rpc: SharedRpcHandle) -> Self {
        Self {
            rpc,
            snapshot_uuid: None,
            clone_name: None,
            clone_uuid: None,
        }
    }
    pub fn with_snapshot_uuid(mut self, snapshot_uuid: &str) -> Self {
        self.snapshot_uuid = Some(snapshot_uuid.to_owned());
        self
    }
    pub fn with_clone_name(mut self, clone_name: &str) -> Self {
        self.clone_name = Some(clone_name.to_owned());
        self
    }
    pub fn with_clone_uuid(mut self, clone_uuid: &str) -> Self {
        self.clone_uuid = Some(clone_uuid.to_owned());
        self
    }
    pub fn rpc(&self) -> SharedRpcHandle {
        self.rpc.clone()
    }
    pub fn snapshot_uuid(&self) -> String {
        self.snapshot_uuid
            .as_ref()
            .expect("snapshot_uuid must be set")
            .clone()
    }
    pub fn clone_name(&self) -> String {
        self.clone_name
            .as_ref()
            .expect("clone_name must be set")
            .clone()
    }
    pub fn clone_uuid(&self) -> String {
        self.clone_uuid
            .as_ref()
            .expect("clone_uuid must be set")
            .clone()
    }
    pub async fn create_snapshot_clone(&mut self) -> Result<Replica, Status> {
        self.rpc()
            .lock()
            .await
            .snapshot
            .create_snapshot_clone(CreateSnapshotCloneRequest {
                snapshot_uuid: self.snapshot_uuid(),
                clone_name: self.clone_name(),
                clone_uuid: self.clone_uuid(),
            })
            .await
            .map(|r| r.into_inner())
    }
    pub async fn get_clones(&self) -> Result<Vec<Replica>, Status> {
        Ok(list_snapshot_clone(self.rpc())
            .await
            .expect("List Clone Failed")
            .into_iter()
            .filter(|s| s.snapshot_uuid == Some(self.snapshot_uuid()))
            .collect::<Vec<_>>())
    }
}
pub async fn list_snapshot_clone(
    rpc: SharedRpcHandle,
) -> Result<Vec<Replica>, Status> {
    rpc.lock()
        .await
        .snapshot
        .list_snapshot_clone(ListSnapshotCloneRequest {
            snapshot_uuid: None,
        })
        .await
        .map(|r| r.into_inner().replicas)
}
