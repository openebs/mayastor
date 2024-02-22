use crate::grpc::GrpcResult;
use io_engine_api::v1::snapshot_rebuild::{
    CreateSnapshotRebuildRequest,
    DestroySnapshotRebuildRequest,
    ListSnapshotRebuildRequest,
    ListSnapshotRebuildResponse,
    SnapshotRebuild,
    SnapshotRebuildRpc,
};
use tonic::{Request, Status};

#[derive(Debug)]
pub struct SnapshotRebuildService {
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    replica_svc: super::replica::ReplicaService,
}

impl SnapshotRebuildService {
    pub fn new(replica_svc: super::replica::ReplicaService) -> Self {
        Self {
            name: String::from("SnapshotRebuildService"),
            replica_svc,
        }
    }
}

#[tonic::async_trait]
impl SnapshotRebuildRpc for SnapshotRebuildService {
    async fn create_snapshot_rebuild(
        &self,
        _request: Request<CreateSnapshotRebuildRequest>,
    ) -> GrpcResult<SnapshotRebuild> {
        GrpcResult::Err(Status::unimplemented(""))
    }
    async fn list_snapshot_rebuild(
        &self,
        _request: Request<ListSnapshotRebuildRequest>,
    ) -> GrpcResult<ListSnapshotRebuildResponse> {
        GrpcResult::Err(Status::unimplemented(""))
    }
    async fn destroy_snapshot_rebuild(
        &self,
        _request: Request<DestroySnapshotRebuildRequest>,
    ) -> GrpcResult<()> {
        GrpcResult::Err(Status::unimplemented(""))
    }
}
