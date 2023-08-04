use crate::{
    bdev::PtplFileOps,
    bdev_api::BdevError,
    core::{
        logical_volume::LogicalVolume,
        Bdev,
        CloneXattrs,
        Protocol,
        Share,
        ShareProps,
        UntypedBdev,
        UpdateProps,
    },
    grpc::{rpc_submit, GrpcClientContext, GrpcResult, Serializer},
    lvs::{Error as LvsError, Lvol, LvolSpaceUsage, Lvs, LvsLvol},
};
use ::function_name::named;
use futures::FutureExt;
use mayastor_api::v1::replica::*;
use nix::errno::Errno;
use std::{convert::TryFrom, panic::AssertUnwindSafe, pin::Pin};
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct ReplicaService {
    #[allow(unused)]
    name: String,
    client_context:
        std::sync::Arc<tokio::sync::Mutex<Option<GrpcClientContext>>>,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for ReplicaService
where
    T: Send + 'static,
    F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
{
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        let mut context_guard = self.client_context.lock().await;

        // Store context as a marker of to detect abnormal termination of the
        // request. Even though AssertUnwindSafe() allows us to
        // intercept asserts in underlying method strategies, such a
        // situation can still happen when the high-level future that
        // represents gRPC call at the highest level (i.e. the one created
        // by gRPC server) gets cancelled (due to timeout or somehow else).
        // This can't be properly intercepted by 'locked' function itself in the
        // first place, so the state needs to be cleaned up properly
        // upon subsequent gRPC calls.
        if let Some(c) = context_guard.replace(ctx) {
            warn!("{}: gRPC method timed out, args: {}", c.id, c.args);
        }

        let fut = AssertUnwindSafe(f).catch_unwind();
        let r = fut.await;

        // Request completed, remove the marker.
        let ctx = context_guard.take().expect("gRPC context disappeared");

        match r {
            Ok(r) => r,
            Err(_e) => {
                warn!("{}: gRPC method panicked, args: {}", ctx.id, ctx.args);
                Err(Status::cancelled(format!(
                    "{}: gRPC method panicked",
                    ctx.id
                )))
            }
        }
    }
}

impl From<LvolSpaceUsage> for ReplicaSpaceUsage {
    fn from(u: LvolSpaceUsage) -> Self {
        Self {
            capacity_bytes: u.capacity_bytes,
            allocated_bytes: u.allocated_bytes,
            cluster_size: u.cluster_size,
            num_clusters: u.num_clusters,
            num_allocated_clusters: u.num_allocated_clusters,
            allocated_bytes_snapshots: u.allocated_bytes_snapshots,
            num_allocated_clusters_snapshots: u
                .num_allocated_clusters_snapshots,
            allocated_bytes_snapshot_from_clone: u
                .allocated_bytes_snapshot_from_clone,
        }
    }
}

impl From<Lvol> for Replica {
    fn from(l: Lvol) -> Self {
        let usage = l.usage();
        let source_uuid =
            Lvol::get_blob_xattr(&l, CloneXattrs::SourceUuid.name());
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
        }
    }
}

impl Default for ReplicaService {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaService {
    pub fn new() -> Self {
        Self {
            name: String::from("ReplicaSvc"),
            client_context: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}
fn filter_replicas_by_replica_type(
    replicas: Vec<Replica>,
    replica_type: Option<ReplicaType>,
) -> Vec<Replica> {
    let Some(replica_type) = replica_type else {
        return replicas
    };
    replicas
        .into_iter()
        .filter_map(|r| match replica_type {
            // AllReplicas
            ReplicaType::AllReplicas => Some(r),
            // AllReplicasExceptSnapshots
            ReplicaType::AllReplicasExceptSnapshots => {
                if !r.is_snapshot {
                    Some(r)
                } else {
                    None
                }
            }
            // OnlySnapshotClones
            ReplicaType::OnlySnapshotClones => {
                if r.is_clone {
                    Some(r)
                } else {
                    None
                }
            }
            // OnlyReplicas
            ReplicaType::OnlyReplicas => {
                if !r.is_snapshot && !r.is_clone {
                    Some(r)
                } else {
                    None
                }
            }
        })
        .collect()
}
#[tonic::async_trait]
impl ReplicaRpc for ReplicaService {
    #[named]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async move {

            let args = request.into_inner();
            info!("{:?}", args);
            if !matches!(
                Protocol::try_from(args.share)?,
                Protocol::Off | Protocol::Nvmf
            ) {
                return Err(LvsError::ReplicaShareProtocol {
                    value: args.share,
                }).map_err(Status::from);
            }

            let rx = rpc_submit(async move {
                let lvs = match Lvs::lookup_by_uuid(&args.pooluuid) {
                    Some(lvs) => lvs,
                    None => {
                        // lookup takes care of backward compatibility
                        match Lvs::lookup(&args.pooluuid) {
                            Some(lvs) => lvs,
                            None => {
                                return Err(LvsError::Invalid {
                                    source: Errno::ENOMEDIUM,
                                    msg: format!("Pool {} not found", args.pooluuid),
                                })
                            }
                        }
                    }
                };
                // if pooltype is not Lvs, the provided replica uuid need to be added as
                // a metadata on the volume.
                match lvs.create_lvol(&args.name, args.size, Some(&args.uuid), args.thin).await {
                    Ok(mut lvol)
                    if Protocol::try_from(args.share)? == Protocol::Nvmf => {
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
                        match Pin::new(&mut lvol).share_nvmf(Some(props)).await {
                            Ok(s) => {
                                debug!("created and shared {:?} as {}", lvol, s);
                                Ok(Replica::from(lvol))
                            }
                            Err(e) => {
                                debug!(
                                    "failed to share created lvol {:?}: {} (destroying)",
                                    lvol,
                                    e.to_string()
                                );
                                let _ = lvol.destroy().await;
                                Err(e)
                            }
                        }
                    }
                    Ok(lvol) => {
                        debug!("created lvol {:?}", lvol);
                        Ok(Replica::from(lvol))
                    }
                    Err(e) => Err(e),
                }
            })?;
            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        }).await
    }

    #[named]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async {
            let args = request.into_inner();
            info!("{:?}", args);
            let rx = rpc_submit::<_, _, LvsError>(async move {
                // todo: is there still a race here, can the pool be exported
                //   right after the check here and before we
                //   probe for the replica?
                let lvs = match &args.pool {
                    Some(destroy_replica_request::Pool::PoolUuid(uuid)) => {
                        Lvs::lookup_by_uuid(uuid)
                            .ok_or(LvsError::RepDestroy {
                                source: Errno::ENOMEDIUM,
                                name: args.uuid.to_owned(),
                                msg: format!("Pool uuid={uuid} is not loaded"),
                            })
                            .map(Some)
                    }
                    Some(destroy_replica_request::Pool::PoolName(name)) => {
                        Lvs::lookup(name)
                            .ok_or(LvsError::RepDestroy {
                                source: Errno::ENOMEDIUM,
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
                        source: Errno::ENOENT,
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
                            source: Errno::EMEDIUMTYPE,
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
        })
        .await
    }

    #[named]
    async fn list_replicas(
        &self,
        request: Request<ListReplicaOptions>,
    ) -> GrpcResult<ListReplicasResponse> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let rx = rpc_submit::<_, _, LvsError>(async move {
                let mut lvols = Vec::new();
                if let Some(bdev) = UntypedBdev::bdev_first() {
                    lvols = bdev
                        .into_iter()
                        .filter(|b| b.driver() == "lvol")
                        .map(|b| Lvol::try_from(b).unwrap())
                        .collect();
                }

                // perform filtering on lvols
                if let Some(pool_name) = args.poolname {
                    lvols.retain(|l| l.pool_name() == pool_name);
                }
                // perform filtering on lvols
                if let Some(pool_uuid) = args.pooluuid {
                    lvols.retain(|l| l.pool_uuid() == pool_uuid);
                }

                // convert lvols to replicas
                let mut replicas: Vec<Replica> =
                    lvols.into_iter().map(Replica::from).collect();

                // perform the filtering on the replica list
                if let Some(name) = args.name {
                    replicas.retain(|r| r.name == name);
                } else if let Some(uuid) = args.uuid {
                    replicas.retain(|r| r.uuid == uuid);
                }
                let replicas = filter_replicas_by_replica_type(
                    replicas,
                    ReplicaType::from_i32(args.replicatype),
                );
                Ok(ListReplicasResponse {
                    replicas,
                })
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit(async move {
                    match Bdev::lookup_by_uuid_str(&args.uuid) {
                        Some(bdev) => {
                            let mut lvol = Lvol::try_from(bdev)?;

                            // if we are already shared with the same protocol
                            if lvol.shared()
                                == Some(Protocol::try_from(args.share)?)
                            {
                                Pin::new(&mut lvol)
                                    .update_properties(
                                        UpdateProps::new().with_allowed_hosts(
                                            args.allowed_hosts,
                                        ),
                                    )
                                    .await?;
                                return Ok(Replica::from(lvol));
                            }

                            match Protocol::try_from(args.share)? {
                                Protocol::Off => {
                                    return Err(LvsError::Invalid {
                                        source: Errno::EINVAL,
                                        msg: "invalid share protocol NONE"
                                            .to_string(),
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
                                    Pin::new(&mut lvol)
                                        .share_nvmf(Some(props))
                                        .await?;
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
            },
        )
            .await
    }

    #[named]
    async fn unshare_replica(
        &self,
        request: Request<UnshareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
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
            },
        )
        .await
    }
}
