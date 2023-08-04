use crate::{
    bdev::{
        nexus,
        nexus::{NexusReplicaSnapshotDescriptor, NexusReplicaSnapshotStatus},
    },
    core::{
        lock::ProtectedSubsystems,
        logical_volume::LogicalVolume,
        snapshot::{
            SnapshotDescriptor,
            SnapshotOps,
            SnapshotParams,
            SnapshotXattrs,
            VolumeSnapshotDescriptor,
        },
        ResourceLockManager,
        UntypedBdev,
    },
    grpc::{
        rpc_submit,
        v1::nexus::nexus_lookup,
        GrpcClientContext,
        GrpcResult,
        Serializer,
    },
    lvs::{Error as LvsError, Lvol, Lvs, LvsLvol},
    spdk_rs::ffihelper::IntoCString,
};
use ::function_name::named;
use chrono::{DateTime, Utc};
use core::ffi::{c_char, c_void};
use futures::FutureExt;
use mayastor_api::v1::snapshot::*;
use nix::errno::Errno;
use spdk_rs::libspdk::spdk_blob_get_xattr_value;
use std::{convert::TryFrom, panic::AssertUnwindSafe};
use strum::IntoEnumIterator;
use tonic::{Request, Response, Status};

/// Support for the snapshot's consumption as source, should be marked as true
/// once we start supporting the feature.
const SNAPSHOT_READY_AS_SOURCE: bool = false;

#[derive(Debug)]
#[allow(dead_code)]
pub struct SnapshotService {
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

impl From<NexusCreateSnapshotReplicaDescriptor>
    for NexusReplicaSnapshotDescriptor
{
    fn from(descr: NexusCreateSnapshotReplicaDescriptor) -> Self {
        NexusReplicaSnapshotDescriptor {
            replica_uuid: descr.replica_uuid,
            snapshot_uuid: descr.snapshot_uuid,
            skip: descr.skip,
        }
    }
}
impl From<NexusReplicaSnapshotStatus> for NexusCreateSnapshotReplicaStatus {
    fn from(status: NexusReplicaSnapshotStatus) -> Self {
        Self {
            replica_uuid: status.replica_uuid,
            status_code: status.status,
        }
    }
}

/// Generate SnapshotInfo for the CreateSnapshot Response.
impl From<ReplicaSnapshotDescriptor> for SnapshotInfo {
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
                SnapshotXattrs::SnapshotCreateTime => {
                    snapshot_param.set_create_time(curr_attr_val);
                }
                SnapshotXattrs::DiscardedSnapshot => {
                    snapshot_param.set_discarded_snapshot(
                        curr_attr_val.parse().unwrap_or_default(),
                    );
                }
            }
        }

        let usage = snap_lvol.usage();

        Self {
            snapshot_uuid: snap_lvol.uuid(),
            snapshot_name: snap_lvol.name(),
            snapshot_size: usage.allocated_bytes,
            num_clones: 0, //TODO: Need to implement along with clone
            timestamp: snapshot_param
                .create_time()
                .map(|s| s.parse::<DateTime<Utc>>().unwrap_or_default().into()),
            source_uuid: r.replica_uuid,
            source_size: r.replica_size,
            pool_uuid: snap_lvol.pool_uuid(),
            pool_name: snap_lvol.pool_name(),
            entity_id: snapshot_param.entity_id().unwrap_or_default(),
            txn_id: snapshot_param.txn_id().unwrap_or_default(),
            valid_snapshot: true,
            ready_as_source: SNAPSHOT_READY_AS_SOURCE,
            referenced_bytes: match usage.allocated_bytes_snapshot_from_clone {
                Some(size) => size,
                _ => usage.allocated_bytes_snapshots,
            },
            discarded_snapshot: snapshot_param.discarded_snapshot(),
        }
    }
}

/// Generate SnapshotInfo for the ListSnapshot Response.
impl From<VolumeSnapshotDescriptor> for SnapshotInfo {
    fn from(s: VolumeSnapshotDescriptor) -> Self {
        let usage = s.snapshot_lvol().usage();

        Self {
            snapshot_uuid: s.snapshot_lvol().uuid(),
            snapshot_name: s.snapshot_params().name().unwrap_or_default(),
            snapshot_size: usage.allocated_bytes,
            num_clones: s.num_clones(),
            timestamp: s
                .snapshot_params()
                .create_time()
                .map(|s| s.parse::<DateTime<Utc>>().unwrap_or_default().into()),
            source_uuid: s.source_uuid(),
            source_size: s.snapshot_lvol().size(),
            pool_uuid: s.snapshot_lvol().pool_uuid(),
            pool_name: s.snapshot_lvol().pool_name(),
            entity_id: s.snapshot_params().entity_id().unwrap_or_default(),
            txn_id: s.snapshot_params().txn_id().unwrap_or_default(),
            valid_snapshot: s.valid_snapshot(),
            ready_as_source: SNAPSHOT_READY_AS_SOURCE,
            referenced_bytes: match usage.allocated_bytes_snapshot_from_clone {
                Some(size) => size,
                _ => usage.allocated_bytes_snapshots,
            },
            discarded_snapshot: s.snapshot_params().discarded_snapshot(),
        }
    }
}
#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for SnapshotService
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
impl Default for SnapshotService {
    fn default() -> Self {
        Self::new()
    }
}
impl SnapshotService {
    pub fn new() -> Self {
        Self {
            name: String::from("SnapshotSvc"),
            client_context: tokio::sync::Mutex::new(None),
        }
    }
    async fn serialized<T, F>(
        &self,
        ctx: GrpcClientContext,
        nexus_uuid: String,
        global_operation: bool,
        f: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
    {
        let lock_manager = ResourceLockManager::get_instance();
        let fut = AssertUnwindSafe(f).catch_unwind();

        // Schedule a Tokio task to detach it from the high-level gRPC future
        // and avoid task cancellation when the top-level gRPC future is
        // cancelled.
        match tokio::spawn(async move {
            // Grab global operation lock, if requested.
            let _global_guard = if global_operation {
                match lock_manager.lock(Some(ctx.timeout)).await {
                    Some(g) => Some(g),
                    None => return Err(Status::deadline_exceeded(
                        "Failed to acquire access to object within given timeout"
                        .to_string()
                    )),
                }
            } else {
                None
            };

            // Grab per-object lock before executing the future.
            let _resource_guard = match lock_manager
                .get_subsystem(ProtectedSubsystems::NEXUS)
                .lock_resource(nexus_uuid, Some(ctx.timeout))
                .await {
                    Some(g) => g,
                    None => return Err(Status::deadline_exceeded(
                        "Failed to acquire access to object within given timeout"
                        .to_string()
                    )),
                };
            let r = fut.await;

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
        })
        .await {
            Ok(r) => r,
            Err(_) => Err(Status::cancelled("gRPC call cancelled"))
        }
    }
}

/// Filter snapshots based on snapshot_query_type came in gRPC request.
fn filter_snapshots_by_snapshot_query_type(
    snapshot_list: Vec<SnapshotInfo>,
    snap_query_type: Option<SnapshotQueryType>,
) -> Vec<SnapshotInfo> {
    let Some(snap_query_type) = snap_query_type else {
        return snapshot_list
    };
    snapshot_list
        .into_iter()
        .filter_map(|s| match snap_query_type {
            // AllSnapshots
            SnapshotQueryType::AllSnapshots => Some(s),
            // AllSnapshotsExceptDiscardedSnapshots
            SnapshotQueryType::AllSnapshotsExceptDiscardedSnapshots => {
                if !s.discarded_snapshot {
                    Some(s)
                } else {
                    None
                }
            }
            // OnlyDiscardedSnapshots
            SnapshotQueryType::OnlyDiscardedSnapshots => {
                if s.discarded_snapshot {
                    Some(s)
                } else {
                    None
                }
            }
            // OnlyInvalidSnapshots
            SnapshotQueryType::OnlyInvalidSnapshots => {
                if !s.valid_snapshot {
                    Some(s)
                } else {
                    None
                }
            }
            // OnlyUsableSnapshots
            SnapshotQueryType::OnlyUsableSnapshots => {
                if !s.discarded_snapshot && s.valid_snapshot {
                    Some(s)
                } else {
                    None
                }
            }
        })
        .collect()
}
#[tonic::async_trait]
impl SnapshotRpc for SnapshotService {
    #[named]
    async fn create_nexus_snapshot(
        &self,
        request: Request<NexusCreateSnapshotRequest>,
    ) -> GrpcResult<NexusCreateSnapshotResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
            info!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let snapshot = SnapshotParams::new(
                    Some(args.entity_id.clone()),
                    Some(args.nexus_uuid.clone()),
                    Some(args.txn_id.clone()),
                    Some(args.snapshot_name.clone()),
                    None, // Snapshot UUID will be handled on per-replica base.
                    Some(Utc::now().to_string()),
                    false,
                );

                let mut nexus = nexus_lookup(&args.nexus_uuid)?;
                let replicas = args
                    .replicas
                    .iter()
                    .cloned()
                    .map(NexusReplicaSnapshotDescriptor::from)
                    .collect::<Vec<_>>();

                let res =
                    nexus.as_mut().create_snapshot(snapshot, replicas).await?;

                let replicas_done = res
                    .replicas_done
                    .into_iter()
                    .map(NexusCreateSnapshotReplicaStatus::from)
                    .collect::<Vec<_>>();
                info!("Create Snapshot Success for {nexus:?}, {replicas_done:?}, replicas_skipped: {:?}", res.replicas_skipped);
                Ok(NexusCreateSnapshotResponse {
                    nexus: Some(nexus.into_grpc().await),
                    snapshot_timestamp: res
                        .snapshot_timestamp
                        .map(|x| x.into()),
                    replicas_done,
                    replicas_skipped: res.replicas_skipped,
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
                    if UntypedBdev::lookup_by_uuid_str(&args.snapshot_uuid).is_some() {
                        return Err(LvsError::Invalid {
                            source: Errno::EEXIST,
                            msg: format!(
                                "Snapshot {} already exist in the system",
                                args.snapshot_uuid
                            ),
                        })
                    }
                    // prepare snap config before taking snapshot.
                    let snap_config =
                        match lvol.prepare_snap_config(
                            &args.snapshot_name,
                            &args.entity_id,
                            &args.txn_id,
                            &args.snapshot_uuid
                        ) {
                            Some(snap_config) => snap_config,
                            // if any of the prepare parameters not passed,
                            // return failure as invalid argument.
                            None => return Err(LvsError::Invalid {
                                source: Errno::EINVAL,
                                msg: format!(
                                    "Snapshot {} some parameters not provided",
                                    args.snapshot_uuid
                                ),
                            })
                        };
                    let replica_uuid = lvol.uuid();
                    let replica_size = lvol.size();
                    // create snapshot
                    match lvol.create_snapshot(snap_config).await {
                        Ok(snap_lvol) => {
                            info!("Create Snapshot Success for {lvol:?}, {snap_lvol:?}");
                            let snapshot_descriptor =
                                ReplicaSnapshotDescriptor::new(snap_lvol, replica_uuid, replica_size);
                            Ok(CreateReplicaSnapshotResponse {
                                replica_uuid: lvol.uuid(),
                                snapshot: Some(SnapshotInfo::from(snapshot_descriptor)),
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
    async fn list_snapshot(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> GrpcResult<ListSnapshotsResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                let rx = rpc_submit(async move {
                    let snapshots: Vec<SnapshotInfo>;
                    // if snapshot_uuid is input, get specific snapshot result
                    if let Some(ref snapshot_uuid) = args.snapshot_uuid {
                        let lvol = match UntypedBdev::lookup_by_uuid_str(
                            snapshot_uuid,
                        ) {
                            Some(bdev) => Lvol::try_from(bdev)?,
                            None => {
                                return Err(LvsError::Invalid {
                                    source: Errno::ENOENT,
                                    msg: format!(
                                        "Replica {snapshot_uuid} not found",
                                    ),
                                })
                            }
                        };
                        snapshots = lvol
                            .list_snapshot_by_snapshot_uuid()
                            .into_iter()
                            .map(SnapshotInfo::from)
                            .collect();
                    } else if let Some(ref replica_uuid) = args.source_uuid {
                        // if replica_uuid is valid, filter snapshot based
                        // on source_uuid
                        let lvol =
                            match UntypedBdev::lookup_by_uuid_str(replica_uuid)
                            {
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
                        snapshots = lvol
                            .list_snapshot_by_source_uuid()
                            .into_iter()
                            .map(SnapshotInfo::from)
                            .collect();
                    } else {
                        // if source_uuid is not input, list all snapshot
                        // present in system
                        snapshots = Lvol::list_all_snapshots()
                            .into_iter()
                            .map(SnapshotInfo::from)
                            .collect();
                    }
                    let snapshots = filter_snapshots_by_snapshot_query_type(
                        snapshots,
                        SnapshotQueryType::from_i32(args.snapshot_query_type),
                    );
                    Ok(ListSnapshotsResponse {
                        snapshots,
                    })
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
    async fn destroy_snapshot(
        &self,
        request: Request<DestroySnapshotRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit(async move {
                    let lvs = match &args.pool {
                        Some(destroy_snapshot_request::Pool::PoolUuid(uuid)) => {
                            Lvs::lookup_by_uuid(uuid)
                                .ok_or(LvsError::RepDestroy {
                                    source: Errno::ENOMEDIUM,
                                    name: args.snapshot_uuid.to_owned(),
                                    msg: format!(
                                        "Pool uuid={uuid} is not loaded"
                                    ),
                                })
                                .map(Some)
                        }
                        Some(destroy_snapshot_request::Pool::PoolName(name)) => {
                            Lvs::lookup(name)
                                .ok_or(LvsError::RepDestroy {
                                    source: Errno::ENOMEDIUM,
                                    name: args.snapshot_uuid.to_owned(),
                                    msg: format!(
                                        "Pool name={name} is not loaded"
                                    ),
                                })
                                .map(Some)
                        }
                        None => {
                            // back-compat, we keep existing behaviour.
                            Ok(None)
                        }
                    }?;
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
                    if let Some(lvs) = lvs {
                        if lvs.name() != device.pool_name()
                            || lvs.uuid() != device.pool_uuid()
                        {
                            let msg = format!(
                                "Specified {lvs:?} does match the target {device:?}!"
                            );
                            tracing::error!("{msg}");
                            return Err(LvsError::RepDestroy {
                                source: Errno::EMEDIUMTYPE,
                                name: args.snapshot_uuid,
                                msg,
                            });
                        }
                    }
                    device.destroy_snapshot().await?;
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

    #[named]
    async fn create_snapshot_clone(
        &self,
        request: Request<CreateSnapshotCloneRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit(async move {
                    let snap_lvol = match UntypedBdev::lookup_by_uuid_str(
                        &args.snapshot_uuid,
                    ) {
                        Some(bdev) => Lvol::try_from(bdev)?,
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
                    if UntypedBdev::lookup_by_uuid_str(&args.clone_uuid).is_some() {
                        return Err(LvsError::Invalid {
                            source: Errno::EEXIST,
                            msg: format!(
                                "clone uuid {} already exist",
                                args.clone_uuid
                            ),
                        })
                    };
                    // reject clone creation if "discardedSnapshot" xattr is marked as true.
                    if snap_lvol.is_discarded_snapshot() {
                        return Err(LvsError::Invalid {
                            source: Errno::ENOENT,
                            msg: format!(
                                "Snapshot {} is marked to be deleted",
                                args.snapshot_uuid
                            ),
                        })
                    }
                    // prepare clone config.
                    let clone_config =
                        match snap_lvol.prepare_clone_config(
                            &args.clone_name,
                            &args.clone_uuid,
                            &args.snapshot_uuid
                        ) {
                            Some(clone_config) => clone_config,
                            None => return Err(LvsError::Invalid {
                                source: Errno::EINVAL,
                                msg: format!(
                                    "Invalid parameters clone_uuid: {}, clone_name: {}",
                                    args.clone_uuid,
                                    args.clone_name
                                ),
                            })
                        };
                    // create clone.
                    match snap_lvol.create_clone(clone_config).await {
                        Ok(clone_lvol) => {
                            info!("Create Clone Success for {snap_lvol:?}, {clone_lvol:?}");
                            Ok(Replica::from(clone_lvol))
                        }
                        Err(e) => {
                            error!(
                                "Create clone Failed for snapshot: {snap_lvol:?} with Error: {e:?}",
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
    async fn list_snapshot_clone(
        &self,
        request: Request<ListSnapshotCloneRequest>,
    ) -> GrpcResult<ListSnapshotCloneResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                let rx = rpc_submit(async move {
                    // if snapshot_uuid is present, list clones for specific
                    // snapshot_uuid
                    if let Some(snapshot_uuid) = args.snapshot_uuid {
                        let snap_lvol = match UntypedBdev::lookup_by_uuid_str(
                            &snapshot_uuid,
                        ) {
                            Some(bdev) => Lvol::try_from(bdev)?,
                            None => {
                                return Err(LvsError::Invalid {
                                    source: Errno::ENOENT,
                                    msg: format!(
                                        "Snapshot {snapshot_uuid} not found",
                                    ),
                                })
                            }
                        };
                        let replicas = snap_lvol
                            .list_clones_by_snapshot_uuid()
                            .into_iter()
                            .map(Replica::from)
                            .collect();
                        Ok(ListSnapshotCloneResponse {
                            replicas,
                        })
                    } else {
                        // if source_uuid is not input, list all clones
                        // present in system
                        let replicas = Lvol::list_all_clones()
                            .into_iter()
                            .map(Replica::from)
                            .collect();
                        Ok(ListSnapshotCloneResponse {
                            replicas,
                        })
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
