use crate::{
    bdev::PtplFileOps,
    bdev_api::BdevError,
    core::{
        logical_volume::LogicalVolume,
        snapshot::{
            SnapshotDescriptor,
            VolumeSnapshotDescriptor,
            VolumeSnapshotDescriptors,
        },
        Bdev,
        Protocol,
        Share,
        ShareProps,
        SnapshotOps,
        SnapshotParams,
        SnapshotXattrs,
        UntypedBdev,
        UpdateProps,
    },
    grpc::{rpc_submit, GrpcClientContext, GrpcResult, Serializer},
    lvs::{Error as LvsError, Lvol, LvolSpaceUsage, Lvs, LvsLvol},
    spdk_rs::ffihelper::IntoCString,
};
use ::function_name::named;
use core::ffi::{c_char, c_void};
use futures::FutureExt;
use mayastor_api::v1::replica::*;
use nix::errno::Errno;
use spdk_rs::libspdk::spdk_blob_get_xattr_value;
use std::{convert::TryFrom, panic::AssertUnwindSafe, pin::Pin};
use strum::IntoEnumIterator;
use tonic::{Request, Response, Status};

#[derive(Debug)]
#[allow(dead_code)]
pub struct ReplicaService {
    name: String,
    client_context: tokio::sync::Mutex<Option<GrpcClientContext>>,
}
#[derive(Debug)]
pub struct ReplicaSnapshotDescriptor {
    pub snapshot_lvol: Lvol,
    pub replica_uuid: String,
    pub replica_size: u64,
}
impl ReplicaSnapshotDescriptor {
    fn new(
        snapshot_lvol: Lvol,
        replica_uuid: String,
        replica_size: u64,
    ) -> Self {
        Self {
            snapshot_lvol,
            replica_uuid,
            replica_size,
        }
    }
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
        }
    }
}

impl From<Lvol> for Replica {
    fn from(l: Lvol) -> Self {
        let usage = l.usage();
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
        }
    }
}

impl Default for ReplicaService {
    fn default() -> Self {
        Self::new()
    }
}

impl From<VolumeSnapshotDescriptor> for ReplicaSnapshot {
    fn from(s: VolumeSnapshotDescriptor) -> Self {
        Self {
            snapshot_uuid: s.snapshot_uuid().to_string(),
            snapshot_name: s.snapshot_params().name().unwrap_or_default(),
            snapshot_size: s.snapshot_size(),
            num_clones: s.num_clones(),
            timestamp: None, //TODO: Need to update xAttr to track timestamp
            replica_uuid: s.snapshot_params().parent_id().unwrap_or_default(),
            replica_size: s.replica_size(),
            entity_id: s.snapshot_params().entity_id().unwrap_or_default(),
            txn_id: s.snapshot_params().txn_id().unwrap_or_default(),
            valid_snapshot: s.valid_snapshot(),
        }
    }
}

impl From<ReplicaSnapshotDescriptor> for ReplicaSnapshot {
    fn from(r: ReplicaSnapshotDescriptor) -> Self {
        let snap_lvol = r.snapshot_lvol;
        let blob = snap_lvol.bs_iter_first();
        let mut snapshot_param: SnapshotParams = Default::default();
        for attr in SnapshotXattrs::iter() {
            let mut val: *const libc::c_char = std::ptr::null::<libc::c_char>();
            let mut size: u64 = 0;
            let attr_id = attr.name().to_string().into_cstring();
            let curr_attr_val = unsafe {
                let _r = spdk_blob_get_xattr_value(
                    blob,
                    attr_id.as_ptr(),
                    &mut val as *mut *const c_char as *mut *const c_void,
                    &mut size as *mut u64,
                );

                let sl =
                    std::slice::from_raw_parts(val as *const u8, size as usize);
                std::str::from_utf8(sl).map_or_else(|error| {
                    warn!(
                        snapshot=snap_lvol.name(),
                        attribute=attr.name(),
                        ?error,
                        "Failed to parse snapshot attribute, default to empty string"
                    );
                    String::default()
                },
                |v| v.to_string())
            };
            match attr {
                SnapshotXattrs::ParentId => {
                    snapshot_param.set_parent_id(curr_attr_val);
                }
                SnapshotXattrs::EntityId => {
                    snapshot_param.set_entity_id(curr_attr_val);
                }
                SnapshotXattrs::TxId => {
                    snapshot_param.set_txn_id(curr_attr_val);
                }
                SnapshotXattrs::SnapshotUuid => {
                    snapshot_param.set_snapshot_uuid(curr_attr_val);
                }
            }
        }
        Self {
            snapshot_uuid: snap_lvol.uuid(),
            snapshot_name: snap_lvol.name(),
            snapshot_size: snap_lvol.size(),
            num_clones: 0, //TODO: Need to implement along with clone
            timestamp: None, //TODO: Need to update xAttr to track timestamp
            replica_uuid: r.replica_uuid,
            replica_size: r.replica_size,
            entity_id: snapshot_param.entity_id().unwrap_or_default(),
            txn_id: snapshot_param.txn_id().unwrap_or_default(),
            valid_snapshot: true,
        }
    }
}
impl ReplicaService {
    pub fn new() -> Self {
        Self {
            name: String::from("ReplicaSvc"),
            client_context: tokio::sync::Mutex::new(None),
        }
    }
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
                        msg: "".into(),
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
                lvol.destroy().await?;
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
    #[named]
    async fn create_replica_snapshot(
        &self,
        request: Request<CreateReplicaSnapshotRequest>,
    ) -> GrpcResult<CreateReplicaSnapshotResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit(async move {
                    let lvol = match UntypedBdev::lookup_by_uuid_str(
                        &args.replica_uuid,
                    ) {
                        Some(bdev) => Lvol::try_from(bdev)?,
                        None => {
                            return Err(LvsError::Invalid {
                                source: Errno::ENOENT,
                                msg: format!(
                                    "Replica {} not found",
                                    args.replica_uuid
                                ),
                            })
                        }
                    };
                    // validate snapshot name
                    if let Some(_r) = UntypedBdev::lookup_by_name(&args.snapshot_name) {
                        return Err(LvsError::Invalid {
                            source: Errno::EEXIST,
                            msg: format!(
                                "Snapshot name {} already exist",
                                args.snapshot_name
                            ),
                        })
                    }
                    // validate snapshot uuid
                    if let Some(_r) = UntypedBdev::lookup_by_uuid_str(&args.snapshot_uuid) {
                        return Err(LvsError::Invalid {
                            source: Errno::EEXIST,
                            msg: format!(
                                "Snapshot uuid {} already exist",
                                args.snapshot_uuid
                            ),
                        })
                    }
                    // prepare snap config and flush IO before taking snapshot.
                    let Some(snap_config) =
                        lvol.prepare_snap_config(
                            &args.snapshot_name,
                            &args.entity_id,
                            &args.txn_id,
                            &args.snapshot_uuid
                        ) else {
                            return Err(LvsError::Invalid {
                                source: Errno::EINVAL,
                                msg: format!(
                                    "tx id / snapshot name not provided for replica {}",
                                    args.replica_uuid
                                ),
                            });
                    };
                    let replica_uuid = lvol.uuid();
                    let replica_size = lvol.size();
                    // create snapshot
                    match lvol.create_snapshot(snap_config.clone()).await {
                        Ok(snap_lvol) => {
                            info!("Create Snapshot Success for {lvol:?}, {snap_lvol:?}");
                            let snapshot_descriptor =
                                ReplicaSnapshotDescriptor::new(snap_lvol, replica_uuid, replica_size);
                            Ok(CreateReplicaSnapshotResponse {
                                replica_uuid: lvol.uuid(),
                                snapshot: Some(ReplicaSnapshot::from(snapshot_descriptor)),
                            })
                        }
                        Err(e) => {
                            error!(
                                "Create Snapshot Failed for lvol: {lvol:?} with Error: {e:?}",
                            );
                            Err(e)
                        }
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
    async fn list_replica_snapshot(
        &self,
        request: Request<ListReplicaSnapshotsRequest>,
    ) -> GrpcResult<ListReplicaSnapshotsResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit(async move {
                    let replica_uuid = args.replica_uuid;
                    match replica_uuid {
                        // if replica_uuid is valid, filter snapshot based on
                        // replica_uuid
                        Some(replica_uuid) => {
                            let lvol = match UntypedBdev::lookup_by_uuid_str(
                                &replica_uuid,
                            ) {
                                Some(bdev) => Lvol::try_from(bdev)?,
                                None => {
                                    return Err(LvsError::Invalid {
                                        source: Errno::ENOENT,
                                        msg: format!(
                                            "Replica {replica_uuid} not found",
                                        ),
                                    })
                                }
                            };
                            let snapshots = lvol
                                .list_snapshot()
                                .into_iter()
                                .map(ReplicaSnapshot::from)
                                .collect();
                            Ok(ListReplicaSnapshotsResponse {
                                snapshots,
                            })
                        }
                        // if replica_uuid is not input, list all snapshot
                        // present in system
                        None => {
                            let snapshots = Lvol::list_all_snapshots()
                                .into_iter()
                                .map(ReplicaSnapshot::from)
                                .collect();
                            Ok(ListReplicaSnapshotsResponse {
                                snapshots,
                            })
                        }
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
    async fn delete_replica_snapshot(
        &self,
        request: Request<DeleteReplicaSnapshotRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit(async move {
                    let bdev = UntypedBdev::bdev_first()
                        .expect("Failed to enumerate devices");

                    let device = match bdev
                        .into_iter()
                        .find(|b| {
                            b.driver() == "lvol"
                                && b.uuid_as_string() == args.snapshot_uuid
                        })
                        .map(|b| Lvol::try_from(b).unwrap())
                    {
                        Some(lvol) => lvol,
                        None => {
                            return Err(LvsError::Invalid {
                                source: Errno::ENOENT,
                                msg: format!(
                                    "Snapshot {} not found",
                                    args.snapshot_uuid
                                ),
                            })
                        }
                    };
                    device.destroy().await?;
                    Ok(())
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
