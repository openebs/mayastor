use crate::{
    bdev::{
        nexus,
        nexus::{NexusReplicaSnapshotDescriptor, NexusReplicaSnapshotStatus},
    },
    core::{
        lock::ProtectedSubsystems,
        snapshot::{SnapshotDescriptor, SnapshotParams},
        ResourceLockManager,
        UntypedBdev,
    },
    grpc::{
        rpc_submit,
        v1::{nexus::nexus_lookup, replica::ReplicaGrpc},
        GrpcClientContext,
        GrpcResult,
        RWSerializer,
    },
};
use ::function_name::named;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use io_engine_api::v1::snapshot::*;
use std::panic::AssertUnwindSafe;
use tonic::{Request, Response, Status};

/// Support for the snapshot's consumption as source, should be marked as true
/// once we start supporting the feature.
const SNAPSHOT_READY_AS_SOURCE: bool = false;

#[derive(Debug)]
#[allow(dead_code)]
pub struct SnapshotService {
    name: String,
    replica_svc: super::replica::ReplicaService,
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

/// Generate SnapshotInfo for the ListSnapshot Response.
impl From<SnapshotDescriptor> for SnapshotInfo {
    fn from(s: SnapshotDescriptor) -> Self {
        let usage = s.snapshot().usage();
        let info = s.info();
        let params = info.snapshot_params();

        Self {
            snapshot_uuid: s.snapshot().uuid(),
            snapshot_name: info.snapshot_params().name().unwrap_or_default(),
            snapshot_size: usage.allocated_bytes,
            num_clones: info.num_clones(),
            timestamp: params
                .create_time()
                .map(|s| s.parse::<DateTime<Utc>>().unwrap_or_default().into()),
            source_uuid: info.source_uuid(),
            source_size: s.snapshot().size(),
            pool_uuid: s.snapshot().pool_uuid(),
            pool_name: s.snapshot().pool_name(),
            entity_id: params.entity_id().unwrap_or_default(),
            txn_id: params.txn_id().unwrap_or_default(),
            valid_snapshot: info.valid_snapshot(),
            ready_as_source: SNAPSHOT_READY_AS_SOURCE,
            referenced_bytes: match usage.allocated_bytes_snapshot_from_clone {
                Some(size) => size,
                _ => usage.allocated_bytes_snapshots,
            },
            discarded_snapshot: params.discarded_snapshot(),
        }
    }
}

impl From<ListSnapshotsRequest> for ListSnapshotArgs {
    fn from(value: ListSnapshotsRequest) -> Self {
        Self {
            uuid: value.snapshot_uuid,
            source_uuid: value.source_uuid,
        }
    }
}
impl From<ListSnapshotCloneRequest> for ListCloneArgs {
    fn from(value: ListSnapshotCloneRequest) -> Self {
        Self {
            snapshot_uuid: value.snapshot_uuid,
        }
    }
}

#[async_trait::async_trait]
impl<F, T> RWSerializer<F, T> for SnapshotService
where
    T: Send + 'static,
    F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
{
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        self.replica_svc.locked(ctx, f).await
    }
    async fn shared(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        self.replica_svc.shared(ctx, f).await
    }
}

impl SnapshotService {
    pub fn new(replica_svc: super::replica::ReplicaService) -> Self {
        Self {
            name: String::from("SnapshotSvc"),
            replica_svc,
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
                match lock_manager.lock(Some(ctx.timeout), false).await {
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
                .lock_resource(nexus_uuid, Some(ctx.timeout), false)
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

/// Filter snapshots based on query came in gRPC request.
fn filter_snapshots_by_snapshot_query_type(
    snapshot_list: Vec<SnapshotInfo>,
    query: Option<list_snapshots_request::Query>,
) -> Vec<SnapshotInfo> {
    let query = match query {
        None => return snapshot_list,
        Some(query) => query,
    };

    snapshot_list
        .into_iter()
        .filter(|snapshot| {
            let query = &query;

            let query_fields = [
                (query.invalid, snapshot.valid_snapshot),
                (query.discarded, snapshot.discarded_snapshot),
                // ... add other fields here as needed
            ];

            query_fields.iter().all(|(query_field, snapshot_field)| {
                match query_field {
                    Some(true) => *snapshot_field,
                    Some(false) => !(*snapshot_field),
                    None => true,
                }
            })
        })
        .collect()
}

use crate::{
    core::snapshot::ISnapshotDescriptor,
    grpc::v1::{pool::PoolGrpc, replica::GrpcReplicaFactory},
    replica_backend::{
        FindReplicaArgs,
        FindSnapshotArgs,
        ListCloneArgs,
        ListSnapshotArgs,
        ReplicaFactory,
        SnapshotOps,
    },
};

impl ReplicaGrpc {
    async fn create_snapshot(
        &mut self,
        args: CreateReplicaSnapshotRequest,
    ) -> Result<CreateReplicaSnapshotResponse, tonic::Status> {
        let replica = &mut self.replica;
        // prepare snap config before taking snapshot.
        let snap_config = match replica.prepare_snap_config(
            &args.snapshot_name,
            &args.entity_id,
            &args.txn_id,
            &args.snapshot_uuid,
        ) {
            Some(snap_config) => snap_config,
            // if any of the prepare parameters not passed
            // return failure as invalid argument.
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Snapshot {} some parameters not provided",
                    args.snapshot_uuid
                )));
            }
        };

        // todo: shouldn't these be in the interfaces themselves? Or perhaps
        //  some validation function.
        if UntypedBdev::lookup_by_uuid_str(&args.snapshot_uuid).is_some() {
            // todo: double check this error, may not be a snapshot, could be
            //  another bdev!!
            return Err(tonic::Status::already_exists(format!(
                "Snapshot {} already exist in the system",
                args.snapshot_uuid
            )));
        }

        match replica.create_snapshot(snap_config).await {
            Ok(snap_lvol) => {
                info!("Create Snapshot Success for {replica:?}, {snap_lvol:?}");

                Ok(CreateReplicaSnapshotResponse {
                    replica_uuid: replica.uuid(),
                    snapshot: snap_lvol.descriptor().map(SnapshotInfo::from),
                })
            }
            Err(error) => {
                error!(
                    "Create Snapshot Failed for lvol: {replica:?} with Error: {error:?}"
                );
                Err(error.into())
            }
        }
    }
}

/// A wrapper over a `SnapshotOps`.
struct SnapshotGrpc(Box<dyn SnapshotOps>);
impl SnapshotGrpc {
    async fn finder(args: &FindSnapshotArgs) -> Result<Self, Status> {
        let mut error = None;

        for factory in ReplicaFactory::factories() {
            match factory.as_factory().find_snap(args).await {
                Ok(Some(snapshot)) => {
                    return Ok(Self(snapshot));
                }
                Ok(None) => {}
                Err(err) => {
                    error = Some(Status::from(err));
                }
            }
        }
        Err(error.unwrap_or_else(|| {
            Status::not_found(format!("Snapshot {args:?} not found"))
        }))
    }
    fn verify_pool(&self, pool: &PoolGrpc) -> Result<(), Status> {
        let snapshot = &self.0;
        let pool = pool.as_ops();
        if pool.name() != snapshot.pool_name()
            || pool.uuid() != snapshot.pool_uuid()
        {
            let msg = format!(
                "Specified pool: {pool:?} does not match the target snapshot's pool: {snapshot:?}!"
            );
            tracing::error!("{msg}");
            // todo: is this the right error code?
            //  keeping for back compatibility
            return Err(Status::aborted(msg));
        }
        Ok(())
    }
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
                crate::spdk_submit!(async move {
                    let args = request.into_inner();
                    info!("{:?}", args);

                    let probe = FindReplicaArgs::new(&args.replica_uuid);
                    let mut replica =
                        GrpcReplicaFactory::finder(&probe).await?;
                    replica.create_snapshot(args).await
                })
            },
        )
        .await
    }
    #[named]
    async fn list_snapshot(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> GrpcResult<ListSnapshotsResponse> {
        self.shared(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    let args = request.into_inner();
                    trace!("{:?}", args);

                    let fargs = ListSnapshotArgs::from(args.clone());
                    let mut snapshots = vec![];
                    for factory in GrpcReplicaFactory::factories() {
                        if let Ok(fsnapshots) = factory.list_snaps(&fargs).await
                        {
                            snapshots.extend(fsnapshots);
                        }
                    }

                    Ok(ListSnapshotsResponse {
                        snapshots: filter_snapshots_by_snapshot_query_type(
                            snapshots, args.query,
                        ),
                    })
                })
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
                crate::spdk_submit!(async move {
                    let pool = match &args.pool {
                        Some(pool) => {
                            Some(GrpcReplicaFactory::pool_finder(pool).await?)
                        }
                        None => None,
                    };
                    let probe = FindSnapshotArgs::new(args.snapshot_uuid);
                    let snapshot = SnapshotGrpc::finder(&probe).await?;
                    if let Some(pool) = &pool {
                        SnapshotGrpc::verify_pool(&snapshot, pool)?;
                    }

                    snapshot.0.destroy_snapshot().await?;
                    Ok(())
                })
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
                crate::spdk_submit!(async move {
                    if UntypedBdev::lookup_by_uuid_str(&args.clone_uuid).is_some() {
                        return Err(tonic::Status::already_exists(format!("clone uuid {} already exist", args.clone_uuid)));
                    }
                    let probe = FindSnapshotArgs::new(args.snapshot_uuid.clone());
                    let snapshot =
                        SnapshotGrpc::finder(&probe).await?.0;

                    // reject clone creation if "discardedSnapshot" xattr is marked as true.
                    // todo: should be part of create_clone?
                    if snapshot.discarded() {
                        return Err(tonic::Status::not_found(format!(
                            "Snapshot {} is marked to be deleted",
                            args.snapshot_uuid
                        )));
                    }

                    let clone_config =
                        match snapshot.prepare_clone_config(
                            &args.clone_name,
                            &args.clone_uuid,
                            &args.snapshot_uuid
                        ) {
                            Some(clone_config) => Ok(clone_config),
                            None => Err(tonic::Status::invalid_argument(format!(
                                "Invalid parameters clone_uuid: {}, clone_name: {}",
                                args.clone_uuid,
                                args.clone_name
                            )))
                        }?;
                    match snapshot.create_clone(clone_config).await {
                        Ok(clone_lvol) => {
                            info!("Create Clone Success for {snapshot:?}, {clone_lvol:?}");
                            Ok(Replica::from(clone_lvol))
                        }
                        Err(e) => {
                            error!(
                                "Create clone Failed for snapshot: {snapshot:?} with Error: {e:?}"
                            );
                            Err(e.into())
                        }
                    }
                })
            },
        )
        .await
    }

    #[named]
    async fn list_snapshot_clone(
        &self,
        request: Request<ListSnapshotCloneRequest>,
    ) -> GrpcResult<ListSnapshotCloneResponse> {
        self.shared(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                crate::spdk_submit!(async move {
                    let args = ListCloneArgs::from(args);
                    let mut replicas = vec![];
                    for factory in GrpcReplicaFactory::factories() {
                        if let Ok(clones) = factory.list_clones(&args).await {
                            replicas.extend(clones);
                        }
                    }
                    Ok(ListSnapshotCloneResponse {
                        replicas,
                    })
                })
            },
        )
        .await
    }
}
