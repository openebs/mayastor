pub use crate::pool_backend::FindPoolArgs as PoolIdProbe;
use crate::{
    bdev::crypto::{Cipher, EncryptionKey as PoolEncKey},
    core::{
        BdevErrorStats, MayastorEnvironment, NvmfShareProps, ProtectedSubsystems, Protocol,
        ResourceLockGuard, ResourceLockManager,
    },
    grpc::{acquire_subsystem_lock, GrpcClientContext, GrpcResult, RWLock, RWSerializer},
    lvs::{BsError, LvsError},
    pool_backend::{
        self, FindPoolArgs, IPoolFactory, ListPoolArgs, PoolArgs, PoolBackend, PoolFactory,
        PoolOps, ReplicaArgs,
    },
    pool_information::pool_info_read,
};
use ::function_name::named;
use futures::FutureExt;
use io_engine_api::v1::{
    common::{create_pool_request, import_pool_request, Cipher as GrpcCipher, EncryptionData},
    pool::*,
    replica::destroy_replica_request,
    snapshot::destroy_snapshot_request,
};
use secret_provider::secret_data;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt::Debug,
    ops::Deref,
    panic::AssertUnwindSafe,
};
use tonic::{Code, Request, Status};

trait AsyncFrom<T>: Sized {
    async fn async_from(value: T) -> Self;
}
trait AsyncInto<T>: Sized {
    async fn async_into(self) -> T;
}
impl<T, U> AsyncInto<U> for T
where
    U: AsyncFrom<T>,
{
    async fn async_into(self) -> U {
        U::async_from(self).await
    }
}

pub type PoolCreateEncryptionParams = create_pool_request::Encryption;
pub type PoolImportEncryptionParams = import_pool_request::Encryption;

enum PoolEncryptionParams {
    Create(PoolCreateEncryptionParams),
    Import(PoolImportEncryptionParams),
    NoEncryptionParams,
}

impl From<DestroyPoolRequest> for FindPoolArgs {
    fn from(value: DestroyPoolRequest) -> Self {
        Self::name_uuid(value.name, &value.uuid)
    }
}
impl From<&destroy_replica_request::Pool> for FindPoolArgs {
    fn from(value: &destroy_replica_request::Pool) -> Self {
        match value.clone() {
            destroy_replica_request::Pool::PoolName(name) => Self::NameUuid { name, uuid: None },
            destroy_replica_request::Pool::PoolUuid(uuid) => Self::Uuid(uuid),
        }
    }
}
impl From<&destroy_snapshot_request::Pool> for FindPoolArgs {
    fn from(value: &destroy_snapshot_request::Pool) -> Self {
        match value.clone() {
            destroy_snapshot_request::Pool::PoolName(name) => Self::NameUuid { name, uuid: None },
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
impl From<ClearErrorRequest> for FindPoolArgs {
    fn from(value: ClearErrorRequest) -> Self {
        Self::name_uuid(value.name, &value.uuid)
    }
}

/// Helper routine to extract Encryption params from the Create or Import pool request.
async fn util_fetch_secret_params(
    params: &PoolEncryptionParams,
) -> Result<Option<PoolEncKey>, Status> {
    let enc_key = match params {
        PoolEncryptionParams::Create(enc_arg) => {
            match enc_arg.clone() {
                // Would have been nice to deduplicate the code if we could call secret_data using a
                // trait object returned by library. But it doesn't seem to work here because the
                // library call having generic type in definition isn't allowing that.
                create_pool_request::Encryption::Data(ckd) => Some(
                    ckd.try_into()
                        .map_err(|e: LvsError| Status::invalid_argument(e.to_string()))?,
                ),
                create_pool_request::Encryption::Secret(cks) => {
                    let mut secret_params: PoolEncKey = secret_data(cks.secret.as_str())
                        .await
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    trace!("[create pool] Received encryption params: {secret_params:?}");
                    // Use secret name as key name.
                    secret_params.key_name = cks.secret.to_string();
                    Some(secret_params)
                }
            }
        }
        PoolEncryptionParams::Import(enc_arg) => match enc_arg.clone() {
            import_pool_request::Encryption::Data(ikd) => Some(
                ikd.try_into()
                    .map_err(|e: LvsError| Status::invalid_argument(e.to_string()))?,
            ),
            import_pool_request::Encryption::Secret(iks) => {
                let mut secret_params: PoolEncKey = secret_data(iks.secret.as_str())
                    .await
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                trace!("[import pool] Received encryption params: {secret_params:?}");
                // Use secret file name as key name.
                secret_params.key_name = iks.secret.to_string();
                Some(secret_params)
            }
        },
        PoolEncryptionParams::NoEncryptionParams => None,
    };

    Ok(enc_key)
}

/// RPC service for mayastor pool operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PoolService {
    name: String,
    client_context: std::sync::Arc<tokio::sync::RwLock<Option<GrpcClientContext>>>,
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

impl TryFrom<EncryptionData> for PoolEncKey {
    type Error = LvsError;
    fn try_from(msg: EncryptionData) -> Result<Self, Self::Error> {
        let key = if let Some(k) = msg.key {
            k
        } else {
            return Err(LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: "missing key".to_string(),
            });
        };

        let arg_cipher = msg.cipher;
        let ctype: Cipher = GrpcCipher::try_from(arg_cipher)
            .map_err(|_| LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: format!("invalid cipher provided: {arg_cipher}"),
            })?
            .into();

        Ok(Self {
            cipher: ctype,
            key_name: key.key_name,
            key: String::from_utf8_lossy(&key.key).to_string(),
            key_len: key.key_length,
            key2: key.key2.map(|k2| String::from_utf8_lossy(&k2).to_string()),
            key2_len: key.key2_length,
        })
    }
}

impl TryFrom<CreatePoolRequest> for PoolArgs {
    type Error = LvsError;
    fn try_from(args: CreatePoolRequest) -> Result<Self, Self::Error> {
        if args.disks.is_empty() {
            return Err(LvsError::Invalid {
                source: BsError::InvalidArgument {},
                msg: "missing devices".to_string(),
            });
        }

        let backend = PoolType::try_from(args.pooltype).map_err(|_| LvsError::Invalid {
            source: BsError::InvalidArgument {},
            msg: format!("invalid pooltype provided: {}", args.pooltype),
        })?;
        if backend == PoolType::Lvs {
            if let Some(s) = args.uuid.clone() {
                let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| LvsError::Invalid {
                    source: BsError::InvalidArgument {},
                    msg: format!("invalid uuid provided, {e}"),
                })?;
            }
        }

        Ok(Self {
            name: args.name.clone(),
            disks: args.disks.clone(),
            uuid: args.uuid.clone(),
            cluster_size: args.cluster_size,
            md_args: args.md_args.map(|md| md.into()),
            backend: backend.into(),
            enc_key: None,
            crypto_vbdev_name: None,
            no_spdk: false,
        })
    }
}
impl From<PoolMetadataArgs> for pool_backend::PoolMetadataArgs {
    fn from(params: PoolMetadataArgs) -> Self {
        Self {
            max_expansion: params.max_expansion,
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
                msg: "missing devices".to_string(),
            });
        }

        let backend = PoolType::try_from(args.pooltype).map_err(|_| LvsError::Invalid {
            source: BsError::InvalidArgument {},
            msg: format!("invalid pooltype provided: {}", args.pooltype),
        })?;
        if backend == PoolType::Lvs {
            if let Some(ref s) = args.uuid {
                let _uuid = uuid::Uuid::parse_str(s).map_err(|e| LvsError::Invalid {
                    source: BsError::InvalidArgument {},
                    msg: format!("invalid uuid provided, {e}"),
                })?;
            }
        }

        let enc_key = match args.encryption.clone() {
            Some(import_pool_request::Encryption::Data(kd)) => PoolEncKey::try_from(kd).ok(),
            Some(import_pool_request::Encryption::Secret(_ks)) => None,
            _ => None,
        };

        Ok(Self {
            name: args.name.clone(),
            disks: args.disks.clone(),
            uuid: args.uuid.clone(),
            cluster_size: None,
            md_args: None,
            backend: backend.into(),
            enc_key,
            crypto_vbdev_name: args
                .encryption
                .as_ref()
                .map(|_| format!("crypto_{}", args.name)),
            no_spdk: false,
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
        Self { pool, _guard }
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
                wipe_super: true,
                ..Default::default()
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
                        Ok(io_engine_api::v1::replica::Replica::from(replica.deref()))
                    }
                    Err(error) => {
                        warn!("failed to share created lvol {replica:?}: {error} (destroying)");
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
    async fn clear_errors(&self) -> Result<(), tonic::Status> {
        self.pool.reset_errors().await?;
        Ok(())
    }
    /// Access the `PoolOps` from this wrapper.
    pub(crate) fn as_ops(&self) -> &dyn PoolOps {
        self.pool.deref()
    }
}

struct PoolErrorsNt(PoolErrors);
impl Deref for PoolErrorsNt {
    type Target = PoolErrors;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PoolErrorsNt {
    fn new(pool: &str, environ: &MayastorEnvironment, stats: &BdevErrorStats) -> Self {
        let mut io_stalled = false;
        let mut io_stall_transition_count: u64 = 0;

        let cache = pool_info_read();

        if let Some(pool_lock) = cache.get(pool) {
            let mut pool_mut = pool_lock.write();
            pool_mut.update_transition_timestamp(*environ.pool_args.io_stall_transition_window);
            io_stalled = pool_mut.io_stalled;
            io_stall_transition_count = pool_mut.transition_timestamps.len() as u64;
        }

        Self(PoolErrors {
            alerts: None,
            io_error_count: stats.error_count(),
            io_error_threshold: environ.pool_args.io_error_threshold,
            io_stalled,
            io_stall_transition_count,
            io_stall_transition_threshold: environ.pool_args.io_stall_transition_threshold,
        })
    }
    fn with_io_errors(mut self) -> Self {
        match self.io_error_count {
            0 => {}
            errors if errors < self.io_error_threshold => {
                self.set_alert(PoolAlertStatus::Attention, PoolAlert::IoError)
            }
            _ => self.set_alert(PoolAlertStatus::Warning, PoolAlert::IoErrorExc),
        }
        self
    }
    fn with_io_stall(mut self) -> Self {
        if self.io_stalled {
            self.set_alert(PoolAlertStatus::Critical, PoolAlert::IoStalled);
        }

        // todo: add sliding window parameters
        match self.io_stall_transition_count {
            // todo: Notice should be raised on 1 transition.
            0 | 1 => {}
            num_stalls if num_stalls < self.io_stall_transition_threshold => {
                self.set_alert(PoolAlertStatus::Attention, PoolAlert::IoStallIntermittent)
            }
            _ => self.set_alert(PoolAlertStatus::Warning, PoolAlert::IoStallIntermittentExc),
        }
        self
    }
    fn lower_status(&mut self, status: PoolAlertStatus) {
        let status = status as i32;
        if status > self.status() {
            self.set_status(status);
        }
    }
    fn set_status(&mut self, status: i32) {
        match self.0.alerts.as_mut() {
            None => {
                let alerts = PoolAlerts {
                    status,
                    ..Default::default()
                };
                self.0.alerts = Some(alerts);
            }
            Some(alerts) => {
                alerts.status = status;
            }
        }
    }
    fn set_alert(&mut self, status: PoolAlertStatus, alert: PoolAlert) {
        match self.0.alerts.as_mut() {
            None => {
                let mut alerts = PoolAlerts {
                    status: status as i32,
                    ..Default::default()
                };
                Self::set_alert_(&mut alerts, status, alert);
                self.0.alerts = Some(alerts);
            }
            Some(alerts) => {
                Self::set_alert_(alerts, status, alert);
                self.lower_status(status);
            }
        }
    }
    fn set_alert_(alerts: &mut PoolAlerts, status: PoolAlertStatus, alert: PoolAlert) {
        match status {
            PoolAlertStatus::Healthy => {
                alerts.notice.push(alert as i32);
            }
            PoolAlertStatus::Attention => {
                alerts.attention.push(alert as i32);
            }
            PoolAlertStatus::Warning => {
                alerts.warning.push(alert as i32);
            }
            PoolAlertStatus::Critical => {
                alerts.critical.push(alert as i32);
            }
        }
    }
    fn status(&self) -> i32 {
        self.0.alerts.as_ref().map(|a| a.status).unwrap_or_default()
    }
    fn state(&self) -> PoolState {
        match &self.alerts {
            Some(alerts) if alerts.status >= PoolAlertStatus::Warning as i32 => {
                PoolState::PoolSuspected
            }
            _ => PoolState::PoolOnline,
        }
    }
    fn build(self) -> PoolErrors {
        self.0
    }
}

/// Convert something which implements [`PoolOps`] to the proto `Pool` type.
pub async fn pool_to_proto(pool: &dyn PoolOps) -> Pool {
    pool.async_into().await
}

impl AsyncFrom<Box<dyn PoolOps>> for Pool {
    async fn async_from(value: Box<dyn PoolOps>) -> Self {
        let value = value.deref();
        value.async_into().await
    }
}
impl AsyncFrom<&dyn PoolOps> for Pool {
    async fn async_from(value: &dyn PoolOps) -> Self {
        let stats = value.error_stats().await.ok();

        let errors = stats.as_ref().map(|stats| {
            let environ = MayastorEnvironment::global();
            PoolErrorsNt::new(value.name(), &environ, stats)
                .with_io_errors()
                .with_io_stall()
        });
        let state = errors.as_ref().map(|errors| errors.state());
        let errors = errors.map(PoolErrorsNt::build);
        Self {
            uuid: value.uuid(),
            name: value.name().into(),
            disks: value.disks(),
            state: state.unwrap_or(PoolState::PoolOnline).into(),
            capacity: value.capacity(),
            used: value.used(),
            committed: value.committed(),
            pooltype: PoolType::from(value.pool_type()) as i32,
            cluster_size: value.cluster_size(),
            page_size: value.page_size(),
            disk_capacity: value.disk_capacity(),
            md_info: value.md_props().map(|md| md.into()),
            encrypted: Some(value.encrypted()),
            max_expandable_size: value.max_expandable_size(),
            disk_info: value
                .disks()
                .into_iter()
                .map(|uri| DiskInfo {
                    uri,
                    errors: errors.clone(),
                })
                .collect(),
            errors,
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
    pub(crate) async fn finder<I: Into<FindPoolArgs>>(args: I) -> Result<PoolGrpc, Status> {
        let pool = PoolFactory::find(args).await?;
        let pool_subsystem =
            ResourceLockManager::get_instance().get_subsystem(ProtectedSubsystems::POOL);
        let lock_guard = acquire_subsystem_lock(pool_subsystem, Some(pool.name())).await?;
        Ok(PoolGrpc::new(pool, lock_guard))
    }
    async fn list(&self, args: &ListPoolArgs) -> Result<Vec<Pool>, Status> {
        let pools = self.as_factory().list(args).await?;
        let mut ret_pools = Vec::with_capacity(pools.len());
        for pool in pools {
            ret_pools.push(pool.async_into().await);
        }
        Ok(ret_pools)
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
        let pool_subsystem =
            ResourceLockManager::get_instance().get_subsystem(ProtectedSubsystems::POOL);
        // todo: missing lock by uuid as well, need to ensure also we don't
        //  clash with a pool with != name but same uuid
        let _lock_guard = acquire_subsystem_lock(pool_subsystem, Some(&args.name)).await?;

        let finder = FindPoolArgs::from(&args);
        for factory in Self::factories() {
            // todo: inspect disk contents as well!
            factory.ensure_not_found(&finder, args.backend).await?;
        }
        let pool = self.as_factory().create(args).await?;
        Ok(pool.async_into().await)
    }
    async fn import(&self, args: PoolArgs) -> Result<Pool, Status> {
        let pool_subsystem =
            ResourceLockManager::get_instance().get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard = acquire_subsystem_lock(pool_subsystem, Some(&args.name)).await?;

        let finder = FindPoolArgs::from(&args);
        for factory in Self::factories() {
            factory.ensure_not_found(&finder, args.backend).await?;
        }
        let pool = self.as_factory().import(args).await?;
        Ok(pool.async_into().await)
    }
    fn as_factory(&self) -> &dyn IPoolFactory {
        self.0.as_factory()
    }
}

#[tonic::async_trait]
impl PoolRpc for PoolService {
    #[named]
    async fn create_pool(&self, request: Request<CreatePoolRequest>) -> GrpcResult<Pool> {
        // Check if the pool is required to be encrypted, and fetch the required
        // encryption parameters from specified source.
        let enc_arg = match request.get_ref().encryption {
            Some(ref e) => PoolEncryptionParams::Create(e.clone()),
            _ => PoolEncryptionParams::NoEncryptionParams,
        };
        let enc_key = util_fetch_secret_params(&enc_arg).await?;

        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());
                    let factory =
                        GrpcPoolFactory::new(PoolBackend::try_from(request.get_ref().pooltype)?)?;

                    factory
                        .create(PoolArgs::try_from(request.into_inner())?.with_encryption(enc_key))
                        .await
                })
            },
        )
        .await
    }

    #[named]
    async fn destroy_pool(&self, request: Request<DestroyPoolRequest>) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let pool = GrpcPoolFactory::finder(request.into_inner()).await?;
                    pool.destroy().await
                })
            },
        )
        .await
    }

    #[named]
    async fn export_pool(&self, request: Request<ExportPoolRequest>) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());

                    let pool = GrpcPoolFactory::finder(request.into_inner()).await?;
                    pool.export().await
                })
            },
        )
        .await
    }

    #[named]
    async fn import_pool(&self, request: Request<ImportPoolRequest>) -> GrpcResult<Pool> {
        // If the pool to be imported is encrypted, fetch the required
        // encryption parameters from specified source.
        let enc_arg = match request.get_ref().encryption {
            Some(ref e) => PoolEncryptionParams::Import(e.clone()),
            _ => PoolEncryptionParams::NoEncryptionParams,
        };
        let enc_key = util_fetch_secret_params(&enc_arg).await?;

        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());
                    let factory =
                        GrpcPoolFactory::new(PoolBackend::try_from(request.get_ref().pooltype)?)?;

                    factory
                        .import(PoolArgs::try_from(request.into_inner())?.with_encryption(enc_key))
                        .await
                })
            },
        )
        .await
    }

    #[named]
    async fn list_pools(&self, request: Request<ListPoolOptions>) -> GrpcResult<ListPoolsResponse> {
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
                            PoolType::try_from(pool_type)
                                .map_err(|_| Status::invalid_argument("Unknown pool type"))?,
                        ),
                    };

                    let args = ListPoolArgs {
                        name: args.name,
                        backend: pool_type.map(Into::into),
                        uuid: args.uuid,
                    };
                    let mut pools = Vec::new();

                    for factory in GrpcPoolFactory::factories() {
                        if args.backend.is_some() && args.backend != Some(factory.backend()) {
                            continue;
                        }
                        match factory.list(&args).await {
                            Ok(fpools) => {
                                pools.extend(fpools);
                            }
                            Err(error) => {
                                let backend = factory.0.as_factory().backend();
                                tracing::error!(
                                    "Failed to list pools of type {backend:?}, error: {error}"
                                );
                            }
                        }
                    }

                    Ok(ListPoolsResponse { pools })
                })
            },
        )
        .await
    }

    async fn grow_pool(&self, _request: Request<GrowPoolRequest>) -> GrpcResult<GrowPoolResponse> {
        Err(Status::new(
            Code::Unimplemented,
            "grow_pool is deprecated. Please use grow_pool_v2",
        ))
    }

    #[named]
    async fn grow_pool_v2(&self, request: Request<GrowPoolRequest>) -> GrpcResult<Pool> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());
                    let pool = GrpcPoolFactory::finder(request.into_inner()).await?;
                    pool.grow().await?;
                    Ok(Pool::async_from(pool.as_ops()).await)
                })
            },
        )
        .await
    }

    #[named]
    async fn clear_errors(&self, request: Request<ClearErrorRequest>) -> GrpcResult<Pool> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                crate::spdk_submit!(async move {
                    info!("{:?}", request.get_ref());
                    let pool = GrpcPoolFactory::finder(request.into_inner()).await?;
                    pool.clear_errors().await?;
                    Ok(Pool::async_from(pool.as_ops()).await)
                })
            },
        )
        .await
    }

    async fn probe_pool(
        &self,
        request: Request<ProbePoolRequest>,
    ) -> GrpcResult<ProbePoolResponse> {
        let request = request.into_inner();

        // todo: implement probes
        if request.probes.is_some() {
            return Err(Status::new(
                Code::InvalidArgument,
                "Pool probes are not implemented",
            ));
        }
        let Some(request) = request.request else {
            return Err(Status::new(
                Code::InvalidArgument,
                "Pool import request is missing",
            ));
        };
        if request.disks.is_empty() {
            return Err(Status::new(
                Code::InvalidArgument,
                "No pool disks specified",
            ));
        }

        let mut errors = HashMap::new();
        for disk_uri in request.disks {
            let parsed = match crate::bdev::uri::try_parse_or_aio(&disk_uri) {
                Ok(parsed) => parsed,
                Err(error) => {
                    let error = vec![ProbeError {
                        code: ProbeErrorCode::InvalidDiskUri as i32,
                        msg: Some(error.to_string()),
                    }];
                    let disk = disk_uri.clone();
                    let info = ProbeDiskInfo { disk, error };
                    errors.insert(disk_uri, info);
                    continue;
                }
            };
            let Err(error) = parsed.probe() else {
                continue;
            };

            let error = vec![error];
            let disk = disk_uri.clone();
            let info = ProbeDiskInfo { disk, error };
            errors.insert(disk_uri, info);
        }

        Ok(tonic::Response::new(ProbePoolResponse {
            success: errors.is_empty(),
            probed: None,
            metadata: None,
            errors,
        }))
    }
}
