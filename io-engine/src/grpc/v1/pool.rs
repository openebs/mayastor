pub use crate::pool_backend::FindPoolArgs as PoolIdProbe;
use crate::{
    core::{
        NvmfShareProps,
        ProtectedSubsystems,
        Protocol,
        ResourceLockGuard,
        ResourceLockManager,
    },
    grpc::{
        acquire_subsystem_lock,
        GrpcClientContext,
        GrpcResult,
        RWLock,
        RWSerializer,
    },
    lvs::{BsError, LvsError},
    pool_backend::{
        self,
        FindPoolArgs,
        IPoolFactory,
        ListPoolArgs,
        PoolArgs,
        PoolBackend,
        PoolFactory,
        PoolOps,
        ReplicaArgs,
    },
};
use ::function_name::named;
use futures::FutureExt;
use io_engine_api::v1::{
    pool::*,
    replica::destroy_replica_request,
    snapshot::destroy_snapshot_request,
};
use std::{convert::TryFrom, fmt::Debug, ops::Deref, panic::AssertUnwindSafe};
use tonic::{Request, Status};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

impl From<DestroyPoolRequest> for FindPoolArgs {
    fn from(value: DestroyPoolRequest) -> Self {
        Self::name_uuid(value.name, &value.uuid)
    }
}
impl From<&destroy_replica_request::Pool> for FindPoolArgs {
    fn from(value: &destroy_replica_request::Pool) -> Self {
        match value.clone() {
            destroy_replica_request::Pool::PoolName(name) => Self::NameUuid {
                name,
                uuid: None,
            },
            destroy_replica_request::Pool::PoolUuid(uuid) => Self::Uuid(uuid),
        }
    }
}
impl From<&destroy_snapshot_request::Pool> for FindPoolArgs {
    fn from(value: &destroy_snapshot_request::Pool) -> Self {
        match value.clone() {
            destroy_snapshot_request::Pool::PoolName(name) => Self::NameUuid {
                name,
                uuid: None,
            },
            destroy_snapshot_request::Pool::PoolUuid(uuid) => Self::Uuid(uuid),
        }
    }
}
impl From<ExportPoolRequest> for FindPoolArgs {
    fn from(value: ExportPoolRequest) -> Self {
        Self::name_uuid(value.name, &value.uuid)
    }
}
impl From<GrowPoolRequest> for FindPoolArgs {
    fn from(value: GrowPoolRequest) -> Self {
        Self::name_uuid(value.name, &value.uuid)
    }
}

/// RPC service for mayastor pool operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PoolService {
    name: String,
    client_context:
        std::sync::Arc<tokio::sync::RwLock<Option<GrpcClientContext>>>,
}

#[async_trait::async_trait]
impl<F, T> RWSerializer<F, T> for PoolService
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
impl RWLock for PoolService {
    async fn rw_lock(&self) -> &tokio::sync::RwLock<Option<GrpcClientContext>> {
        self.client_context.as_ref()
    }
}

impl TryFrom<CreatePoolRequest> for PoolArgs {
    type Error = LvsError;
    fn try_from(args: CreatePoolRequest) -> Result<Self, Self::Error> {
        if args.disks.is_empty() {
            return Err(LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: "invalid argument, missing devices".to_string(),
            });
        }

        let backend = PoolType::try_from(args.pooltype).map_err(|_| {
            LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: format!("invalid pooltype provided: {}", args.pooltype),
            }
        })?;
        if backend == PoolType::Lvs {
            if let Some(s) = args.uuid.clone() {
                let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| {
                    LvsError::Invalid {
                        source: BsError::InvalidArgument {},
                        msg: format!("invalid uuid provided, {e}"),
                    }
                })?;
            }
        }

        Ok(Self {
            name: args.name,
            disks: args.disks,
            uuid: args.uuid,
            cluster_size: args.cluster_size,
            md_args: args.md_args.map(|md| md.into()),
            backend: backend.into(),
        })
    }
}
impl From<PoolMetadataArgs> for pool_backend::PoolMetadataArgs {
    fn from(params: PoolMetadataArgs) -> Self {
        Self {
            md_resv_ratio: params.md_resv_ratio,
        }
    }
}
impl From<PoolType> for PoolBackend {
    fn from(value: PoolType) -> Self {
        match value {
            PoolType::Lvs => Self::Lvs,
            PoolType::Lvm => Self::Lvm,
        }
    }
}
impl From<PoolBackend> for PoolType {
    fn from(value: PoolBackend) -> Self {
        match value {
            PoolBackend::Lvs => Self::Lvs,
            PoolBackend::Lvm => Self::Lvm,
        }
    }
}
impl TryFrom<i32> for PoolBackend {
    type Error = std::io::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match PoolType::try_from(value) {
            Ok(value) => Ok(value.into()),
            Err(_) => Err(Self::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid pool type {value}"),
            )),
        }
    }
}
impl TryFrom<&i32> for PoolBackend {
    type Error = std::io::Error;

    fn try_from(value: &i32) -> Result<Self, Self::Error> {
        Self::try_from(*value)
    }
}

impl TryFrom<ImportPoolRequest> for PoolArgs {
    type Error = LvsError;
    fn try_from(args: ImportPoolRequest) -> Result<Self, Self::Error> {
        if args.disks.is_empty() {
            return Err(LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: "invalid argument, missing devices".to_string(),
            });
        }

        let backend = PoolType::try_from(args.pooltype).map_err(|_| {
            LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: format!("invalid pooltype provided: {}", args.pooltype),
            }
        })?;
        if backend == PoolType::Lvs {
            if let Some(s) = args.uuid.clone() {
                let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| {
                    LvsError::Invalid {
                        source: BsError::InvalidArgument {},
                        msg: format!("invalid uuid provided, {e}"),
                    }
                })?;
            }
        }

        Ok(Self {
            name: args.name,
            disks: args.disks,
            uuid: args.uuid,
            cluster_size: None,
            md_args: None,
            backend: backend.into(),
        })
    }
}

impl Default for PoolService {
    fn default() -> Self {
        Self::new()
    }
}

/// A wrapper over a `PoolOps` with a resource lock guard ensuring pool sync
/// whilst this is in scope.
pub(crate) struct PoolGrpc {
    // todo: the current resource lock might not be sufficient as they do not
    //  protect the pool access in all cases, example: when looking up a
    //  particular replica, we don't have access to the pool name until
    //  we've found the replica, at which point something else might be
    //  trying to delete the pool for example...
    _guard: ResourceLockGuard<'static>,
    pool: Box<dyn PoolOps>,
}

impl PoolGrpc {
    fn new(pool: Box<dyn PoolOps>, _guard: ResourceLockGuard<'static>) -> Self {
        Self {
            pool,
            _guard,
        }
    }
    pub(crate) async fn create_replica(
        &self,
        args: io_engine_api::v1::replica::CreateReplicaRequest,
    ) -> Result<io_engine_api::v1::replica::Replica, Status> {
        let protocol = Protocol::try_from(args.share)?;
        match self
            .pool
            .create_repl(ReplicaArgs {
                name: args.name.to_string(),
                size: args.size,
                uuid: args.uuid,
                thin: args.thin,
                entity_id: args.entity_id,
                use_extent_table: None,
            })
            .await
        {
            Ok(mut replica) if protocol == Protocol::Nvmf => {
                let props = NvmfShareProps::new()
                    .with_allowed_hosts(args.allowed_hosts)
                    .with_ptpl(replica.create_ptpl()?);
                match replica.share_nvmf(props).await {
                    Ok(share_uri) => {
                        debug!("created and shared {replica:?} as {share_uri}");
                        Ok(io_engine_api::v1::replica::Replica::from(
                            replica.deref(),
                        ))
                    }
                    Err(error) => {
                        warn!(
                            "failed to share created lvol {replica:?}: {error} (destroying)"
                        );
                        let _ = replica.destroy().await;
                        Err(error.into())
                    }
                }
            }
            Ok(replica) => {
                debug!("created lvol {:?}", replica);
                Ok(io_engine_api::v1::replica::Replica::from(replica.deref()))
            }
            Err(error) => Err(error.into()),
        }
    }
    async fn destroy(self) -> Result<(), tonic::Status> {
        self.pool.destroy().await?;
        Ok(())
    }
    async fn export(self) -> Result<(), tonic::Status> {
        self.pool.export().await?;
        Ok(())
    }
    async fn grow(&self) -> Result<(), tonic::Status> {
        self.pool.grow().await?;
        Ok(())
    }
    /// Access the `PoolOps` from this wrapper.
    pub(crate) fn as_ops(&self) -> &dyn PoolOps {
        self.pool.deref()
    }
}

impl From<Box<dyn PoolOps>> for Pool {
    fn from(value: Box<dyn PoolOps>) -> Self {
        let value = value.deref();
        value.into()
    }
}
impl From<&dyn PoolOps> for Pool {
    fn from(value: &dyn PoolOps) -> Self {
        Self {
            uuid: value.uuid(),
            name: value.name().into(),
            disks: value.disks(),
            state: PoolState::PoolOnline.into(),
            capacity: value.capacity(),
            used: value.used(),
            committed: value.committed(),
            pooltype: PoolType::from(value.pool_type()) as i32,
            cluster_size: value.cluster_size(),
            page_size: value.page_size(),
            disk_capacity: value.disk_capacity(),
            md_info: value.md_props().map(|md| md.into()),
        }
    }
}
impl From<pool_backend::PoolMetadataInfo> for PoolMetadataInfo {
    fn from(value: pool_backend::PoolMetadataInfo) -> Self {
        Self {
            md_page_size: value.md_page_size,
            md_pages: value.md_pages,
            md_used_pages: value.md_used_pages,
        }
    }
}

impl PoolService {
    pub fn new() -> Self {
        Self {
            name: String::from("PoolSvc"),
            client_context: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
}

impl PoolBackend {
    /// Check if this backend type is enabled.
    pub(crate) fn enabled(&self) -> Result<(), Status> {
        match self {
            PoolBackend::Lvs => Ok(()),
            PoolBackend::Lvm => crate::grpc::lvm_enabled(),
        }
    }
}

/// A pool factory with the various types of specific impls.
pub(crate) struct GrpcPoolFactory(PoolFactory);
impl GrpcPoolFactory {
    pub(crate) fn factories() -> Vec<Self> {
        PoolFactory::factories()
            .into_iter()
            .map(Self)
            .collect::<Vec<_>>()
    }
    fn new(backend: PoolBackend) -> Result<Self, Status> {
        backend.enabled()?;
        Ok(Self(PoolFactory::new(backend)))
    }

    /// Probe backends for the given name and/or uuid and return the right one.
    pub(crate) async fn finder<I: Into<FindPoolArgs>>(
        args: I,
    ) -> Result<PoolGrpc, Status> {
        let pool = PoolFactory::find(args).await?;
        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(pool.name())).await?;
        Ok(PoolGrpc::new(pool, lock_guard))
    }
    async fn list(&self, args: &ListPoolArgs) -> Result<Vec<Pool>, Status> {
        let pools = self.as_factory().list(args).await?;
        Ok(pools.into_iter().map(Into::into).collect::<Vec<_>>())
    }
    /// Lists all `PoolOps` matching the given arguments.
    pub(crate) async fn list_ops(
        &self,
        args: &ListPoolArgs,
    ) -> Result<Vec<Box<dyn PoolOps>>, Status> {
        let pools = self.as_factory().list(args).await?;
        Ok(pools)
    }
    fn backend(&self) -> PoolBackend {
        self.as_factory().backend()
    }
    async fn ensure_not_found(
        &self,
        args: &FindPoolArgs,
        backend: PoolBackend,
    ) -> Result<(), Status> {
        if self.as_factory().find(args).await?.is_some() {
            if self.backend() != backend {
                return Err(Status::invalid_argument(
                    "Pool Already exists on another backend type",
                ));
            }
            // todo: add a better validation here, example if pool already
            // exists, do we return already exists only if all the parameters
            // match and invalid argument or something else otherwise?
            Ok(())
        } else {
            Ok(())
        }
    }
    async fn create(&self, args: PoolArgs) -> Result<Pool, Status> {
        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        // todo: missing lock by uuid as well, need to ensure also we don't
        //  clash with a pool with != name but same uuid
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(&args.name)).await?;

        let finder = FindPoolArgs::from(&args);
        for factory in Self::factories() {
            // todo: inspect disk contents as well!
            factory.ensure_not_found(&finder, args.backend).await?;
        }
        let pool = self.as_factory().create(args).await?;
        Ok(pool.into())
    }
    async fn import(&self, args: PoolArgs) -> Result<Pool, Status> {
        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(&args.name)).await?;

        let finder = FindPoolArgs::from(&args);
        for factory in Self::factories() {
            factory.ensure_not_found(&finder, args.backend).await?;
        }
        let pool = self.as_factory().import(args).await?;
        Ok(pool.into())
    }
    fn as_factory(&self) -> &dyn IPoolFactory {
        self.0.as_factory()
    }
}

#[tonic::async_trait]
impl PoolRpc for PoolService {
    #[named]
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let factory = GrpcPoolFactory::new(PoolBackend::try_from(
                        request.get_ref().pooltype,
                    )?)?;
                    factory
                        .create(PoolArgs::try_from(request.into_inner())?)
                        .await
                })
            },
        )
        .await
    }

    #[named]
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let pool =
                        GrpcPoolFactory::finder(request.into_inner()).await?;
                    pool.destroy().await.map_err(Into::into)
                })
            },
        )
        .await
    }

    #[named]
    async fn export_pool(
        &self,
        request: Request<ExportPoolRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let pool =
                        GrpcPoolFactory::finder(request.into_inner()).await?;
                    pool.export().await.map_err(Into::into)
                })
            },
        )
        .await
    }

    #[named]
    async fn import_pool(
        &self,
        request: Request<ImportPoolRequest>,
    ) -> GrpcResult<Pool> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let factory = GrpcPoolFactory::new(PoolBackend::try_from(
                        request.get_ref().pooltype,
                    )?)?;
                    factory
                        .import(PoolArgs::try_from(request.into_inner())?)
                        .await
                })
            },
        )
        .await
    }

    #[named]
    async fn list_pools(
        &self,
        request: Request<ListPoolOptions>,
    ) -> GrpcResult<ListPoolsResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    let args = request.into_inner();

                    // todo: what is the intent here when None, to only return
                    // pools  of Lvs?
                    // todo: Also, what todo when we hit an error listing any of
                    // the  types? Or should we have
                    // separate lists per type?
                    let pool_type = args.pooltype.as_ref().map(|v| v.value);
                    let pool_type = match pool_type {
                        None => None,
                        Some(pool_type) => Some(
                            PoolType::try_from(pool_type).map_err(|_| {
                                Status::invalid_argument("Unknown pool type")
                            })?,
                        ),
                    };

                    let args = ListPoolArgs {
                        name: args.name,
                        backend: pool_type.map(Into::into),
                        uuid: args.uuid,
                    };
                    let mut pools = Vec::new();

                    for factory in GrpcPoolFactory::factories() {
                        if args.backend.is_some()
                            && args.backend != Some(factory.backend())
                        {
                            continue;
                        }
                        match factory.list(&args).await {
                            Ok(fpools) => {
                                pools.extend(fpools);
                            }
                            Err(error) => {
                                let backend = factory.0.as_factory().backend();
                                tracing::error!("Failed to list pools of type {backend:?}, error: {error}");
                            }
                        }
                    }

                    Ok(ListPoolsResponse {
                        pools,
                    })
                })
            },
        )
        .await
    }

    #[named]
    async fn grow_pool(
        &self,
        request: Request<GrowPoolRequest>,
    ) -> GrpcResult<GrowPoolResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let pool =
                        GrpcPoolFactory::finder(request.into_inner()).await?;

                    let previous_pool = Pool::from(pool.as_ops());
                    pool.grow().await.map_err(Into::<Status>::into)?;
                    let current_pool = Pool::from(pool.as_ops());

                    if current_pool.capacity == previous_pool.capacity {
                        info!(
                            "Grow pool '{p}': capacity did not change: {sz} bytes",
                            p = current_pool.name,
                            sz = current_pool.capacity,
                        );
                    } else{
                        info!(
                            "Grow pool '{p}': pool capacity has changed from {a} to {b} bytes",
                            p = current_pool.name,
                            a = previous_pool.capacity,
                            b = current_pool.capacity
                        );
                    }

                    Ok(GrowPoolResponse {
                        previous_pool: Some(previous_pool),
                        current_pool: Some(current_pool),
                    })
                })
            },
        )
        .await
    }
}
