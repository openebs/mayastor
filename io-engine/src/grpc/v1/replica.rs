use crate::{
    core::{
        logical_volume::LvolSpaceUsage,
        NvmfShareProps,
        ProtectedSubsystems,
        Protocol,
        ResourceLockManager,
        UpdateProps,
    },
    grpc::{
        acquire_subsystem_lock,
        v1::pool::{GrpcPoolFactory, PoolGrpc, PoolIdProbe},
        GrpcClientContext,
        GrpcResult,
        RWLock,
        RWSerializer,
    },
    pool_backend::{FindPoolArgs, PoolBackend},
    replica_backend::{
        FindReplicaArgs,
        ListCloneArgs,
        ListReplicaArgs,
        ListSnapshotArgs,
        ReplicaFactory,
        ReplicaOps,
    },
};
use ::function_name::named;
use futures::FutureExt;
use io_engine_api::v1::{pool::PoolType, replica::*};
use std::{convert::TryFrom, ops::Deref, panic::AssertUnwindSafe};
use tonic::{Request, Status};

#[derive(Debug, Clone)]
pub struct ReplicaService {
    #[allow(unused)]
    name: String,
    client_context:
        std::sync::Arc<tokio::sync::RwLock<Option<GrpcClientContext>>>,
}

#[async_trait::async_trait]
impl<F, T> RWSerializer<F, T> for ReplicaService
where
    T: Send + 'static,
    F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
{
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        let mut context_guard = self.client_context.write().await;

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
    async fn shared(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        let context_guard = self.client_context.read().await;

        if let Some(c) = context_guard.as_ref() {
            warn!("{}: gRPC method timed out, args: {}", c.id, c.args);
        }

        let fut = AssertUnwindSafe(f).catch_unwind();
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
    }
}

#[async_trait::async_trait]
impl RWLock for ReplicaService {
    async fn rw_lock(&self) -> &tokio::sync::RwLock<Option<GrpcClientContext>> {
        self.client_context.as_ref()
    }
}

impl Default for ReplicaService {
    fn default() -> Self {
        Self {
            name: String::from("ReplicaSvc"),
            client_context: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
}

impl From<destroy_replica_request::Pool> for PoolIdProbe {
    fn from(value: destroy_replica_request::Pool) -> Self {
        match value {
            destroy_replica_request::Pool::PoolName(name) => {
                Self::UuidOrName(name)
            }
            destroy_replica_request::Pool::PoolUuid(uuid) => Self::Uuid(uuid),
        }
    }
}
impl From<ListReplicaOptions> for ListReplicaArgs {
    fn from(value: ListReplicaOptions) -> Self {
        ListReplicaArgs {
            name: value.name,
            uuid: value.uuid,
            pool_name: value.poolname,
            pool_uuid: value.pooluuid,
        }
    }
}

impl From<Box<dyn ReplicaOps>> for Replica {
    fn from(value: Box<dyn ReplicaOps>) -> Self {
        let value = value.deref();
        value.into()
    }
}
impl From<&dyn ReplicaOps> for Replica {
    fn from(l: &dyn ReplicaOps) -> Self {
        Self {
            name: l.name(),
            uuid: l.uuid(),
            size: l.size(),
            thin: l.is_thin(),
            share: l.share_protocol() as i32,
            uri: l.bdev_share_uri().unwrap_or_default(),
            poolname: l.pool_name(),
            usage: Some(l.usage().into()),
            allowed_hosts: l.nvmf_allowed_hosts(),
            is_snapshot: l.is_snapshot(),
            is_clone: l.is_clone(),
            pooltype: PoolType::from(l.backend()) as i32,
            pooluuid: l.pool_uuid(),
            snapshot_uuid: l.snapshot_uuid(),
            entity_id: l.entity_id(),
        }
    }
}

fn filter_replicas_by_replica_type(
    replica_list: Vec<Replica>,
    query: Option<list_replica_options::Query>,
) -> Vec<Replica> {
    let query = match query {
        None => return replica_list,
        Some(query) => query,
    };
    replica_list
        .into_iter()
        .filter(|replica| {
            let query = &query;

            let query_fields = [
                (query.replica, (!replica.is_snapshot && !replica.is_clone)),
                (query.snapshot, replica.is_snapshot),
                (query.clone, replica.is_clone),
                // ... add other fields here as needed
            ];

            query_fields.iter().any(|(query_field, replica_field)| {
                match query_field {
                    true => *replica_field,
                    false => false,
                }
            })
        })
        .collect()
}

/// A replica factory with the various types of specific impls.
pub(crate) struct GrpcReplicaFactory {
    repl_factory: Box<dyn ReplicaFactory>,
}
impl GrpcReplicaFactory {
    pub(crate) fn factories() -> Vec<Self> {
        crate::replica_backend::factories()
            .into_iter()
            .map(|repl_factory| Self {
                repl_factory,
            })
            .collect::<Vec<_>>()
    }
    async fn find_ops(
        args: &FindReplicaArgs,
    ) -> Result<Box<dyn ReplicaOps>, Status> {
        let mut error = None;

        for factory in Self::factories() {
            match factory.find_replica(args).await {
                Ok(Some(replica)) => {
                    return Ok(replica);
                }
                Ok(None) => {}
                Err(err) => {
                    error = Some(err);
                }
            }
        }
        Err(error.unwrap_or_else(|| {
            Status::not_found(format!("Replica {args:?} not found"))
        }))
    }
    pub(crate) async fn finder(
        args: &FindReplicaArgs,
    ) -> Result<ReplicaGrpc, Status> {
        let replica = Self::find_ops(args).await?;
        Ok(ReplicaGrpc::new(replica))
    }
    pub(crate) async fn pool_finder<I: Into<FindPoolArgs>>(
        args: I,
    ) -> Result<PoolGrpc, Status> {
        GrpcPoolFactory::finder(args).await.map_err(|error| {
            if error.code() == tonic::Code::NotFound {
                Status::failed_precondition(error.to_string())
            } else {
                error
            }
        })
    }
    async fn find_replica(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, tonic::Status> {
        let replica = self.as_factory().find(args).await?;
        if let Some(replica) = &replica {
            // should this be an error?
            if replica.is_snapshot() {
                return Ok(None);
            }
        }
        Ok(replica)
    }
    async fn list(
        &self,
        args: &ListReplicaArgs,
    ) -> Result<Vec<Replica>, Status> {
        let replicas = self.as_factory().list(args).await?;
        Ok(replicas.into_iter().map(Into::into).collect::<Vec<_>>())
    }
    pub(crate) async fn list_snaps(
        &self,
        args: &ListSnapshotArgs,
    ) -> Result<Vec<SnapshotInfo>, Status> {
        let snapshots = self.as_factory().list_snaps(args).await?;
        Ok(snapshots.into_iter().map(Into::into).collect::<Vec<_>>())
    }
    pub(crate) async fn list_clones(
        &self,
        args: &ListCloneArgs,
    ) -> Result<Vec<Replica>, Status> {
        let clones = self.as_factory().list_clones(args).await?;
        Ok(clones.into_iter().map(Into::into).collect::<Vec<_>>())
    }
    pub(crate) fn backend(&self) -> PoolBackend {
        self.as_factory().backend()
    }
    fn as_factory(&self) -> &dyn ReplicaFactory {
        self.repl_factory.deref()
    }
}

/// A wrapper over a `ReplicaOps`.
pub(crate) struct ReplicaGrpc {
    pub(crate) replica: Box<dyn ReplicaOps>,
}
impl ReplicaGrpc {
    fn new(replica: Box<dyn ReplicaOps>) -> Self {
        Self {
            replica,
        }
    }
    async fn destroy(self) -> Result<(), Status> {
        self.replica.destroy().await?;
        Ok(())
    }
    async fn share(&mut self, args: ShareReplicaRequest) -> Result<(), Status> {
        let pool_name = self.replica.pool_name();
        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(&pool_name)).await?;

        let protocol = Protocol::try_from(args.share)?;
        // if we are already shared with the same protocol
        if self.replica.shared() == Some(protocol) {
            self.replica
                .update_properties(
                    UpdateProps::new().with_allowed_hosts(args.allowed_hosts),
                )
                .await?;
            return Ok(());
        }

        if let Protocol::Off = protocol {
            return Err(Status::invalid_argument(
                "Invalid share protocol NONE",
            ));
        }

        let props = NvmfShareProps::new()
            .with_allowed_hosts(args.allowed_hosts)
            .with_ptpl(self.replica.create_ptpl()?);
        self.replica.share_nvmf(props).await?;
        Ok(())
    }
    async fn unshare(&mut self) -> Result<(), Status> {
        let pool_name = self.replica.pool_name();
        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(&pool_name)).await?;

        if self.replica.shared().is_some() {
            self.replica.unshare().await?;
        }
        Ok(())
    }
    async fn resize(&mut self, resize: u64) -> Result<(), Status> {
        // todo: shouldn't these also take the pool lock?
        self.replica.resize(resize).await?;
        Ok(())
    }
    async fn set_entity_id(&mut self, id: String) -> Result<(), Status> {
        self.replica.set_entity_id(id).await?;
        Ok(())
    }
    fn verify_pool(&self, pool: &PoolGrpc) -> Result<(), Status> {
        let pool = pool.as_ops();
        let replica = &self.replica;
        if pool.name() != replica.pool_name()
            || pool.uuid() != replica.pool_uuid()
        {
            let msg = format!(
                "Specified pool: {pool:?} does not match the target replica's pool: {replica:?}!"
            );
            tracing::error!("{msg}");
            // todo: is this the right error code?
            //  keeping for back compatibility
            return Err(Status::aborted(msg));
        }
        Ok(())
    }
}
impl From<ReplicaGrpc> for Replica {
    fn from(value: ReplicaGrpc) -> Self {
        (*value.replica).into()
    }
}

#[tonic::async_trait]
impl ReplicaRpc for ReplicaService {
    #[named]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    if !matches!(
                        Protocol::try_from(request.get_ref().share)?,
                        Protocol::Off | Protocol::Nvmf
                    ) {
                        return Err(Status::invalid_argument(format!(
                            "invalid replica share protocol value: {}",
                            request.get_ref().share
                        )));
                    }

                    let args = request.into_inner();

                    let pool = GrpcReplicaFactory::pool_finder(
                        FindPoolArgs::uuid_or_name(&args.pooluuid),
                    )
                    .await?;
                    pool.create_replica(args).await
                })
            },
        )
        .await
    }

    #[named]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());
                    let args = request.into_inner();

                    let pool = match &args.pool {
                        Some(pool) => {
                            Some(GrpcReplicaFactory::pool_finder(pool).await?)
                        }
                        None => None,
                    };
                    let probe = FindReplicaArgs::new(&args.uuid);
                    let replica = match GrpcReplicaFactory::finder(&probe).await
                    {
                        Err(mut status)
                            if status.code() == tonic::Code::NotFound =>
                        {
                            status.metadata_mut().insert(
                                "gtm-602",
                                tonic::metadata::MetadataValue::from(0),
                            );
                            Err(status)
                        }
                        _else => _else,
                    }?;
                    if let Some(pool) = &pool {
                        replica.verify_pool(pool)?;
                    }
                    replica.destroy().await?;
                    Ok(())
                })
            },
        )
        .await
    }

    #[named]
    async fn list_replicas(
        &self,
        request: Request<ListReplicaOptions>,
    ) -> GrpcResult<ListReplicasResponse> {
        self.shared(GrpcClientContext::new(&request, function_name!()), async {
            crate::spdk_submit!(async move {
                let args = request.into_inner();
                trace!("{:?}", args);

                let mut replicas = vec![];

                let backends = args
                    .pooltypes
                    .iter()
                    .map(PoolBackend::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                let query = args.query.clone();
                let fargs = ListReplicaArgs::from(args);

                for factory in
                    GrpcReplicaFactory::factories().into_iter().filter(|f| {
                        backends.is_empty() || backends.contains(&f.backend())
                    })
                {
                    if let Ok(freplicas) = factory.list(&fargs).await {
                        replicas.extend(freplicas);
                    }
                }

                Ok(ListReplicasResponse {
                    replicas: filter_replicas_by_replica_type(replicas, query),
                })
            })
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
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let probe = FindReplicaArgs::new(&request.get_ref().uuid);
                    let mut replica =
                        GrpcReplicaFactory::finder(&probe).await?;
                    replica.share(request.into_inner()).await?;
                    Ok(replica.into())
                })
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
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let probe = FindReplicaArgs::new(&request.get_ref().uuid);
                    let mut replica =
                        GrpcReplicaFactory::finder(&probe).await?;
                    replica.unshare().await?;
                    Ok(replica.into())
                })
            },
        )
        .await
    }

    #[named]
    async fn resize_replica(
        &self,
        request: Request<ResizeReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let probe = FindReplicaArgs::new(&request.get_ref().uuid);
                    let mut replica =
                        GrpcReplicaFactory::finder(&probe).await?;
                    replica.resize(request.into_inner().requested_size).await?;
                    Ok(replica.into())
                })
            },
        )
        .await
    }

    #[named]
    async fn set_replica_entity_id(
        &self,
        request: Request<SetReplicaEntityIdRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let probe = FindReplicaArgs::new(&request.get_ref().uuid);
                    let mut replica =
                        GrpcReplicaFactory::finder(&probe).await?;
                    replica
                        .set_entity_id(request.into_inner().entity_id)
                        .await?;
                    Ok(replica.into())
                })
            },
        )
        .await
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
