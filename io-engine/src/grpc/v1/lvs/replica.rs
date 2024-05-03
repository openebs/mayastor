use crate::{
    bdev_api::BdevError,
    core::{
        logical_volume::LogicalVolume,
        Bdev,
        CloneXattrs,
        Share,
        UntypedBdev,
    },
    grpc::{
        rpc_submit_ext,
        v1::{pool::PoolGrpc, replica::ReplicaGrpc},
        GrpcResult,
    },
    lvs::{LvsError, BsError, Lvol, Lvs, LvsLvol},
};
use io_engine_api::v1::{pool::PoolType, replica::*};
use std::convert::TryFrom;
use tonic::{Request, Response, Status};

#[macro_export]
macro_rules! lvs_run {
    ($fut:expr) => {{
        let r = $crate::grpc::rpc_submit_ext2($fut)?;
        r.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map(Response::new)
    }};
}

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
        crate::lvs_run!(async move {
            let args = request.into_inner();
            let lvs = match Lvs::lookup_by_uuid(&args.pooluuid) {
                Some(lvs) => Ok(lvs),
                None => {
                    // lookup takes care of backward compatibility
                    match Lvs::lookup(&args.pooluuid) {
                        Some(lvs) => Ok(lvs),
                        None => Err(LvsError::Invalid {
                            source: BsError::LvsNotFound {},
                            msg: format!("Pool {} not found", args.pooluuid),
                        }),
                    }
                }
            }?;
            PoolGrpc::new(lvs).create_replica(args).await
        })
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        crate::lvs_run!(async move {
            let args = request.into_inner();
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
                    }
                    .into());
                }
            }
            ReplicaGrpc::new(lvol).destroy().await
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
        crate::lvs_run!(async move {
            let args = request.into_inner();
            let replica = match Bdev::lookup_by_uuid_str(&args.uuid) {
                Some(bdev) => Lvol::try_from(bdev),
                None => Err(LvsError::InvalidBdev {
                    source: BdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid.clone(),
                }),
            }?;
            let mut replica = ReplicaGrpc::new(replica);
            replica.share(args).await?;
            Ok(replica.into())
        })
    }

    async fn unshare_replica(
        &self,
        request: Request<UnshareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        crate::lvs_run!(async move {
            let args = request.into_inner();
            let replica = match Bdev::lookup_by_uuid_str(&args.uuid) {
                Some(bdev) => Lvol::try_from(bdev),
                None => Err(LvsError::InvalidBdev {
                    source: BdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid.clone(),
                }),
            }?;
            let mut replica = ReplicaGrpc::new(replica);
            replica.unshare().await?;
            Ok(replica.into())
        })
    }

    async fn resize_replica(
        &self,
        request: Request<ResizeReplicaRequest>,
    ) -> GrpcResult<Replica> {
        crate::lvs_run!(async move {
            let args = request.into_inner();
            let replica = Bdev::lookup_by_uuid_str(&args.uuid)
                .and_then(|b| Lvol::try_from(b).ok())
                .ok_or(LvsError::RepResize {
                    source: BsError::LvolNotFound {},
                    name: args.uuid.to_owned(),
                })?;
            let mut replica = ReplicaGrpc::new(replica);
            replica.resize(args.requested_size).await?;
            Ok(replica.into())
        })
    }

    async fn set_replica_entity_id(
        &self,
        request: Request<SetReplicaEntityIdRequest>,
    ) -> GrpcResult<Replica> {
        crate::lvs_run!(async move {
            let args = request.into_inner();
            let replica = match Bdev::lookup_by_uuid_str(&args.uuid) {
                Some(bdev) => Lvol::try_from(bdev),
                None => Err(LvsError::InvalidBdev {
                    source: BdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid.clone(),
                }),
            }?;
            let mut replica = ReplicaGrpc::new(replica);
            replica.set_entity_id(args.entity_id).await?;
            Ok(replica.into())
        })
    }
}

impl ReplicaService {
    pub(crate) async fn list_lvs_replicas(
        &self,
    ) -> Result<Vec<Replica>, tonic::Status> {
        crate::lvs_run!(async move {
            let Some(bdev) = UntypedBdev::bdev_first() else {
                return Ok(vec![]);
            };

            let lvols = bdev.into_iter().filter_map(Lvol::ok_from);
            Ok(lvols.map(Replica::from).collect())
        })
        .map(|r| r.into_inner())
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
