use crate::{
    bdev::PtplFileOps,
    bdev_api::BdevError,
    core::{
        logical_volume::LogicalVolume,
        Bdev,
        CloneXattrs,
        CoreError,
        ProtectedSubsystems,
        Protocol,
        ResourceLockManager,
        Share,
        ShareProps,
        UntypedBdev,
        UpdateProps,
    },
    grpc::{acquire_subsystem_lock, rpc_submit, rpc_submit_ext, GrpcResult},
    lvs::{BsError, Lvol, Lvs, LvsError, LvsLvol, PropValue},
};
use io_engine_api::v1::{pool::PoolType, replica::*};
use std::{convert::TryFrom, pin::Pin};
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub(crate) struct ReplicaService {}

impl ReplicaService {
    pub(crate) fn new() -> Self {
        Self {}
    }
    /// Probe the LVS Replica service for a replica with the given uuid.
    pub(crate) async fn probe(uuid: &str) -> Result<bool, tonic::Status> {
        let uuid = uuid.to_string();
        let rx = rpc_submit_ext(async move {
            Bdev::lookup_by_uuid_str(&uuid)
                .and_then(|b| Lvol::try_from(b).ok())
                .is_some()
        })?;

        rx.await.map_err(|_| Status::cancelled("cancelled"))
    }
}

#[tonic::async_trait]
impl ReplicaRpc for ReplicaService {
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.create_lvs_replica(request.into_inner()).await
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        self.destroy_lvs_replica(request.into_inner()).await
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
        self.share_lvs_replica(request.into_inner()).await
    }

    async fn unshare_replica(
        &self,
        request: Request<UnshareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.unshare_lvs_replica(request.into_inner()).await
    }

    async fn resize_replica(
        &self,
        request: Request<ResizeReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.resize_lvs_replica(request.into_inner()).await
    }

    async fn set_replica_entity_id(
        &self,
        request: Request<SetReplicaEntityIdRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        info!("{args:?}");
        let rx = rpc_submit::<_, _, LvsError>(async move {
            if let Some(bdev) = UntypedBdev::lookup_by_uuid_str(&args.uuid) {
                let mut lvol = Lvol::try_from(bdev)?;
                Pin::new(&mut lvol)
                    .set(PropValue::EntityId(args.entity_id))
                    .await?;
                Ok(Replica::from(lvol))
            } else {
                Err(LvsError::InvalidBdev {
                    source: BdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid,
                })
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }
}

impl ReplicaService {
    async fn create_lvs_replica(
        &self,
        args: CreateReplicaRequest,
    ) -> GrpcResult<Replica> {
        let rx = rpc_submit(async move {
            let protocol = Protocol::try_from(args.share)?;
            let lvs = match Lvs::lookup_by_uuid(&args.pooluuid) {
                Some(lvs) => lvs,
                None => {
                    // lookup takes care of backward compatibility
                    match Lvs::lookup(&args.pooluuid) {
                        Some(lvs) => lvs,
                        None => {
                            return Err(LvsError::Invalid {
                                source: BsError::LvsNotFound {},
                                msg: format!(
                                    "Pool {} not found",
                                    args.pooluuid
                                ),
                            })
                        }
                    }
                }
            };
            let pool_subsystem = ResourceLockManager::get_instance()
                .get_subsystem(ProtectedSubsystems::POOL);
            let _lock_guard =
                acquire_subsystem_lock(pool_subsystem, Some(lvs.name()))
                    .await
                    .map_err(|_| LvsError::ResourceLockFailed {
                        msg: format!(
                            "resource {}, for pooluuid {}",
                            lvs.name(),
                            args.pooluuid
                        ),
                    })?;
            // if pooltype is not Lvs, the provided replica uuid need to be
            // added as a metadata on the volume.
            match lvs
                .create_lvol(
                    &args.name,
                    args.size,
                    Some(&args.uuid),
                    args.thin,
                    args.entity_id,
                )
                .await
            {
                Ok(mut lvol) if protocol == Protocol::Nvmf => {
                    let props = ShareProps::new()
                        .with_allowed_hosts(args.allowed_hosts)
                        .with_ptpl(lvol.ptpl().create().map_err(|source| {
                            LvsError::LvolShare {
                                source: CoreError::Ptpl {
                                    reason: source.to_string(),
                                },
                                name: lvol.name(),
                            }
                        })?);
                    match Pin::new(&mut lvol).share_nvmf(Some(props)).await {
                        Ok(share_uri) => {
                            debug!(
                                "created and shared {lvol:?} as {share_uri}"
                            );
                            Ok(Replica::from(lvol))
                        }
                        Err(error) => {
                            warn!(
                                "failed to share created lvol {lvol:?}: {error} (destroying)"
                            );
                            let _ = lvol.destroy().await;
                            Err(error)
                        }
                    }
                }
                Ok(lvol) => {
                    debug!("created lvol {:?}", lvol);
                    Ok(Replica::from(lvol))
                }
                Err(error) => Err(error),
            }
        })?;
        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn destroy_lvs_replica(
        &self,
        args: DestroyReplicaRequest,
    ) -> GrpcResult<()> {
        let rx = rpc_submit::<_, _, LvsError>(async move {
            // todo: is there still a race here, can the pool be exported
            //   right after the check here and before we
            //   probe for the replica?
            let lvs = match &args.pool {
                Some(destroy_replica_request::Pool::PoolUuid(uuid)) => {
                    Lvs::lookup_by_uuid(uuid)
                        .ok_or(LvsError::RepDestroy {
                            source: BsError::LvsNotFound {},
                            name: args.uuid.to_owned(),
                            msg: format!("Pool uuid={uuid} is not loaded"),
                        })
                        .map(Some)
                }
                Some(destroy_replica_request::Pool::PoolName(name)) => {
                    Lvs::lookup(name)
                        .ok_or(LvsError::RepDestroy {
                            source: BsError::LvsNotFound {},
                            name: args.uuid.to_owned(),
                            msg: format!("Pool name={name} is not loaded"),
                        })
                        .map(Some)
                }
                None => {
                    // back-compat, we keep existing behaviour.
                    Ok(None)
                }
            }?;

            let lvol = Bdev::lookup_by_uuid_str(&args.uuid)
                .and_then(|b| Lvol::try_from(b).ok())
                .ok_or(LvsError::RepDestroy {
                    source: BsError::LvolNotFound {},
                    name: args.uuid.to_owned(),
                    msg: "Replica not found".into(),
                })?;

            if let Some(lvs) = lvs {
                if lvs.name() != lvol.pool_name()
                    || lvs.uuid() != lvol.pool_uuid()
                {
                    let msg = format!(
                        "Specified {lvs:?} does match the target {lvol:?}!"
                    );
                    tracing::error!("{msg}");
                    return Err(LvsError::RepDestroy {
                        source: BsError::LvsIdMismatch {},
                        name: args.uuid,
                        msg,
                    });
                }
            }
            lvol.destroy_replica().await?;
            Ok(())
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn share_lvs_replica(
        &self,
        args: ShareReplicaRequest,
    ) -> GrpcResult<Replica> {
        let rx = rpc_submit(async move {
            match Bdev::lookup_by_uuid_str(&args.uuid) {
                Some(bdev) => {
                    let mut lvol = Lvol::try_from(bdev)?;
                    let pool_subsystem = ResourceLockManager::get_instance()
                        .get_subsystem(ProtectedSubsystems::POOL);
                    let _lock_guard = acquire_subsystem_lock(
                        pool_subsystem,
                        Some(lvol.lvs().name()),
                    )
                    .await
                    .map_err(|_| {
                        LvsError::ResourceLockFailed {
                            msg: format!(
                                "resource {}, for lvol {:?}",
                                lvol.lvs().name(),
                                lvol
                            ),
                        }
                    })?;

                    // if we are already shared with the same protocol
                    if lvol.shared() == Some(Protocol::try_from(args.share)?) {
                        Pin::new(&mut lvol)
                            .update_properties(
                                UpdateProps::new()
                                    .with_allowed_hosts(args.allowed_hosts),
                            )
                            .await?;
                        return Ok(Replica::from(lvol));
                    }

                    match Protocol::try_from(args.share)? {
                        Protocol::Off => {
                            return Err(LvsError::Invalid {
                                source: BsError::InvalidArgument {},
                                msg: "invalid share protocol NONE".to_string(),
                            })
                        }
                        Protocol::Nvmf => {
                            let props = ShareProps::new()
                                .with_allowed_hosts(args.allowed_hosts)
                                .with_ptpl(lvol.ptpl().create().map_err(
                                    |source| LvsError::LvolShare {
                                        source: crate::core::CoreError::Ptpl {
                                            reason: source.to_string(),
                                        },
                                        name: lvol.name(),
                                    },
                                )?);
                            Pin::new(&mut lvol).share_nvmf(Some(props)).await?;
                        }
                    }

                    Ok(Replica::from(lvol))
                }

                None => Err(LvsError::InvalidBdev {
                    source: BdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid,
                }),
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn unshare_lvs_replica(
        &self,
        args: UnshareReplicaRequest,
    ) -> GrpcResult<Replica> {
        let rx = rpc_submit(async move {
            match Bdev::lookup_by_uuid_str(&args.uuid) {
                Some(bdev) => {
                    let mut lvol = Lvol::try_from(bdev)?;
                    if lvol.shared().is_some() {
                        Pin::new(&mut lvol).unshare().await?;
                    }
                    Ok(Replica::from(lvol))
                }
                None => Err(LvsError::InvalidBdev {
                    source: BdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid,
                }),
            }
        })?;
        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn resize_lvs_replica(
        &self,
        args: ResizeReplicaRequest,
    ) -> GrpcResult<Replica> {
        let rx = rpc_submit::<_, _, LvsError>(async move {
            let mut lvol = Bdev::lookup_by_uuid_str(&args.uuid)
                .and_then(|b| Lvol::try_from(b).ok())
                .ok_or(LvsError::RepResize {
                    source: BsError::LvolNotFound {},
                    name: args.uuid.to_owned(),
                })?;
            let requested_size = args.requested_size;
            lvol.resize_replica(requested_size).await?;
            debug!("resized {:?}", lvol);
            Ok(Replica::from(lvol))
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    pub(crate) async fn list_lvs_replicas(
        &self,
    ) -> Result<Vec<Replica>, tonic::Status> {
        let rx = rpc_submit::<_, _, LvsError>(async move {
            let mut lvols = Vec::new();
            if let Some(bdev) = UntypedBdev::bdev_first() {
                lvols = bdev
                    .into_iter()
                    .filter(|b| b.driver() == "lvol")
                    .map(|b| Lvol::try_from(b).unwrap())
                    .collect();
            }

            // convert lvols to replicas
            let replicas: Vec<Replica> =
                lvols.into_iter().map(Replica::from).collect();

            Ok(replicas)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
    }
}

impl From<Lvol> for Replica {
    fn from(l: Lvol) -> Self {
        let usage = l.usage();
        let source_uuid = Lvol::get_blob_xattr(
            l.blob_checked(),
            CloneXattrs::SourceUuid.name(),
        );
        Self {
            name: l.name(),
            uuid: l.uuid(),
            pooluuid: l.pool_uuid(),
            size: usage.capacity_bytes,
            thin: l.is_thin(),
            share: l.shared().unwrap().into(),
            uri: l.share_uri().unwrap(),
            poolname: l.pool_name(),
            usage: Some(usage.into()),
            allowed_hosts: l.allowed_hosts(),
            is_snapshot: l.is_snapshot(),
            is_clone: l.is_snapshot_clone().is_some(),
            snapshot_uuid: source_uuid,
            pooltype: PoolType::Lvs as i32,
            entity_id: l.entity_id(),
        }
    }
}
