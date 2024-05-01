use crate::{
    bdev::PtplFileOps,
    core::{
        CoreError,
        LogicalVolume,
        MayastorFeatures,
        ProtectedSubsystems,
        Protocol,
        ResourceLockManager,
        ShareProps,
        UpdateProps,
    },
    grpc::{acquire_subsystem_lock, lvm_enabled, GrpcResult},
    lvm,
    lvm::{CmnQueryArgs, Error, QueryArgs},
};
use io_engine_api::v1::{pool::PoolType, replica::*};
use std::convert::TryFrom;
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
        self.create_lvm_replica(request.into_inner()).await
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        self.destroy_lvm_replica(request.into_inner()).await
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
        self.share_lvm_replica(request.into_inner()).await
    }

    async fn unshare_replica(
        &self,
        request: Request<UnshareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.unshare_lvm_replica(request.into_inner()).await
    }

    async fn resize_replica(
        &self,
        request: Request<ResizeReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.resize_lvm_replica(request.into_inner()).await
    }

    async fn set_replica_entity_id(
        &self,
        request: Request<SetReplicaEntityIdRequest>,
    ) -> GrpcResult<Replica> {
        self.set_replica_entity_id(request.into_inner()).await
    }
}

impl ReplicaService {
    async fn create_lvm_replica(
        &self,
        args: CreateReplicaRequest,
    ) -> GrpcResult<Replica> {
        lvm_enabled()?;

        let share = Protocol::try_from(args.share)?;

        let pool =
            lvm::VolumeGroup::lookup(CmnQueryArgs::ours().uuid(&args.pooluuid))
                .await?;

        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(pool.name())).await?;

        let (mut lvol, created) =
            match lvm::LogicalVolume::lookup(&lvm::QueryArgs::new().with_lv(
                CmnQueryArgs::ours().uuid(&args.uuid).named(&args.name),
            ))
            .await
            {
                Ok(lvol) => (lvol, false),
                Err(_) => (
                    lvm::LogicalVolume::create(
                        &args.pooluuid,
                        &args.name,
                        args.size,
                        &args.uuid,
                        args.thin,
                        &args.entity_id,
                        share,
                    )
                    .await?,
                    true,
                ),
            };

        let protocol = Protocol::try_from(args.share)?;
        match protocol {
            Protocol::Nvmf => {
                let props = ShareProps::new()
                    .with_allowed_hosts(args.allowed_hosts)
                    .with_ptpl(lvol.ptpl().create().map_err(|source| {
                        Error::BdevShare {
                            source: CoreError::Ptpl {
                                reason: source.to_string(),
                            },
                        }
                    })?);

                if let Err(error) = lvol.share_nvmf(Some(props)).await {
                    error!("Failed to share lvol: {error}...");
                    if created {
                        // if we have created it here, then let's undo it
                        lvol.destroy().await.ok();
                    }
                    return Err(error.into());
                }
            }
            Protocol::Off => {
                if lvol.share() != Protocol::Off {
                    lvol.unshare().await?;
                }
            }
        }

        if !created {
            return Err(Status::already_exists(format!("{lvol:?}")));
        }

        Ok(Response::new(Replica::from(lvol)))
    }

    async fn destroy_lvm_replica(
        &self,
        args: DestroyReplicaRequest,
    ) -> GrpcResult<()> {
        lvm_enabled()?;

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
        replica.destroy().await?;

        Ok(Response::new(()))
    }

    async fn share_lvm_replica(
        &self,
        args: ShareReplicaRequest,
    ) -> GrpcResult<Replica> {
        lvm_enabled()?;
        let protocol = Protocol::try_from(args.share)?;

        let mut lvol = lvm::LogicalVolume::lookup(
            &QueryArgs::new().with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
        )
        .await?;

        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(lvol.vg_name()))
                .await?;

        // if we are already shared with the same protocol
        if lvol.share_proto() == Some(protocol) {
            lvol.update_share_props(
                UpdateProps::new().with_allowed_hosts(args.allowed_hosts),
            )
            .await?;
            return Ok(Response::new(Replica::from(lvol)));
        }

        match protocol {
            Protocol::Off => {
                return Err(Status::invalid_argument(
                    "invalid share protocol NONE",
                ));
            }
            Protocol::Nvmf => {
                let props = ShareProps::new()
                    .with_allowed_hosts(args.allowed_hosts)
                    .with_ptpl(lvol.ptpl().create().map_err(|source| {
                        Error::BdevShare {
                            source: CoreError::Ptpl {
                                reason: source.to_string(),
                            },
                        }
                    })?);
                lvol.share_nvmf(Some(props)).await?;
            }
        }

        Ok(Response::new(Replica::from(lvol)))
    }

    async fn unshare_lvm_replica(
        &self,
        args: UnshareReplicaRequest,
    ) -> GrpcResult<Replica> {
        lvm_enabled()?;

        let mut lvol = lvm::LogicalVolume::lookup(
            &lvm::QueryArgs::new()
                .with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
        )
        .await?;

        if lvol.share_proto().is_some() {
            lvol.unshare().await?;
        }

        Ok(Response::new(lvol.into()))
    }

    async fn resize_lvm_replica(
        &self,
        args: ResizeReplicaRequest,
    ) -> GrpcResult<Replica> {
        lvm_enabled()?;

        let mut replica = lvm::LogicalVolume::lookup(
            &lvm::QueryArgs::new()
                .with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
        )
        .await?;

        replica.resize(args.requested_size).await?;
        Ok(Response::new(replica.into()))
    }

    async fn set_replica_entity_id(
        &self,
        _args: SetReplicaEntityIdRequest,
    ) -> GrpcResult<Replica> {
        Err(Status::invalid_argument(""))
    }

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
