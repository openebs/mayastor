use crate::{
    core::{LogicalVolume, MayastorFeatures},
    grpc::{
        lvm_enabled,
        v1::{pool::PoolGrpc, replica::ReplicaGrpc},
        GrpcResult,
    },
    lvm,
    lvm::{CmnQueryArgs, Error, QueryArgs},
};
use io_engine_api::v1::{pool::PoolType, replica::*};
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub(crate) struct ReplicaService {}

impl ReplicaService {
    pub(crate) fn new() -> Self {
        Self {}
    }
    /// Probe the LVM Replica service for a replica with the given uuid.
    pub(crate) async fn probe(uuid: &str) -> Result<bool, tonic::Status> {
        if !MayastorFeatures::get_features().lvm() {
            return Ok(false);
        }
        let query = QueryArgs::new().with_lv(CmnQueryArgs::ours().uuid(uuid));
        match lvm::LogicalVolume::lookup(&query).await {
            Ok(_) => Ok(true),
            Err(Error::LvNotFound {
                ..
            }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

#[tonic::async_trait]
impl ReplicaRpc for ReplicaService {
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        lvm_enabled()?;

        crate::lvm_run!(async move {
            let pool = lvm::VolumeGroup::lookup(
                CmnQueryArgs::ours().uuid(&args.pooluuid),
            )
            .await?;

            PoolGrpc::new(pool).create_replica(args).await
        })
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();
        lvm_enabled()?;

        crate::lvm_run!(async move {
            let query =
                QueryArgs::new().with_lv(CmnQueryArgs::ours().uuid(&args.uuid));
            let query = match &args.pool {
                Some(destroy_replica_request::Pool::PoolUuid(uuid)) => {
                    query.with_vg(CmnQueryArgs::ours().uuid(uuid))
                }
                Some(destroy_replica_request::Pool::PoolName(name)) => {
                    query.with_vg(CmnQueryArgs::ours().named(name))
                }
                None => query,
            };

            let replica = lvm::LogicalVolume::lookup(&query).await?;
            ReplicaGrpc::new(replica).destroy().await
        })
    }

    async fn list_replicas(
        &self,
        _request: Request<ListReplicaOptions>,
    ) -> GrpcResult<ListReplicasResponse> {
        unimplemented!("Request is not cloneable, so we have to use another fn")
    }

    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        lvm_enabled()?;

        crate::lvm_run!(async move {
            let replica = lvm::LogicalVolume::lookup(
                &QueryArgs::new()
                    .with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
            )
            .await?;

            let mut replica = ReplicaGrpc::new(replica);
            replica.share(args).await?;
            Ok(replica.into())
        })
    }

    async fn unshare_replica(
        &self,
        request: Request<UnshareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        lvm_enabled()?;

        crate::lvm_run!(async move {
            let replica = lvm::LogicalVolume::lookup(
                &QueryArgs::new()
                    .with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
            )
            .await?;

            let mut replica = ReplicaGrpc::new(replica);
            replica.unshare().await?;
            Ok(replica.into())
        })
    }

    async fn resize_replica(
        &self,
        request: Request<ResizeReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        lvm_enabled()?;

        crate::lvm_run!(async move {
            let replica = lvm::LogicalVolume::lookup(
                &QueryArgs::new()
                    .with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
            )
            .await?;

            let mut replica = ReplicaGrpc::new(replica);
            replica.resize(args.requested_size).await?;
            Ok(replica.into())
        })
    }

    async fn set_replica_entity_id(
        &self,
        request: Request<SetReplicaEntityIdRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        lvm_enabled()?;

        crate::lvm_run!(async move {
            let replica = lvm::LogicalVolume::lookup(
                &QueryArgs::new()
                    .with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
            )
            .await?;

            let mut replica = ReplicaGrpc::new(replica);
            replica.set_entity_id(args.entity_id).await?;
            Ok(replica.into())
        })
    }
}

impl ReplicaService {
    pub(crate) async fn list_lvm_replicas(
        &self,
        args: &ListReplicaOptions,
    ) -> Result<Vec<Replica>, tonic::Status> {
        if !MayastorFeatures::get_features().lvm() {
            return Ok(vec![]);
        }
        let lvols = lvm::LogicalVolume::list(
            &QueryArgs::new()
                .with_lv(
                    CmnQueryArgs::ours()
                        .named_opt(&args.name)
                        .uuid_opt(&args.uuid),
                )
                .with_vg(
                    CmnQueryArgs::ours()
                        .named_opt(&args.poolname)
                        .uuid_opt(&args.pooluuid),
                ),
        )
        .await
        .map_err(Status::from)?;
        Ok(lvols.into_iter().map(Replica::from).collect::<Vec<_>>())
    }
}

// todo: shouldn't this be converted with existing core LogicalVolume trait??
impl From<lvm::LogicalVolume> for Replica {
    fn from(l: lvm::LogicalVolume) -> Self {
        Self {
            name: l.name().clone().unwrap_or_default(),
            uuid: l.uuid().to_string(),
            size: l.size(),
            thin: l.thin(),
            share: l.share().into(),
            uri: l.uri().cloned().unwrap_or_default(),
            poolname: l.vg_name().to_string(),
            usage: Some(l.usage().into()),
            allowed_hosts: l.allowed_hosts().cloned().unwrap_or_default(),
            is_snapshot: false,
            is_clone: false,
            pooltype: PoolType::Lvm as i32,
            pooluuid: l.vg_uuid().to_string(),
            snapshot_uuid: None,
            entity_id: l.entity_id().map(Clone::clone),
        }
    }
}
