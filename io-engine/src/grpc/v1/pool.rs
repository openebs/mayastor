use crate::{
    grpc::{GrpcClientContext, GrpcResult, RWLock, RWSerializer},
    lvs::Error as LvsError,
    pool_backend::{PoolArgs, PoolBackend},
};
use ::function_name::named;
use futures::FutureExt;
use io_engine_api::v1::pool::*;
use nix::errno::Errno;
use std::{convert::TryFrom, fmt::Debug, panic::AssertUnwindSafe};
use tonic::{Request, Response, Status};

use super::{
    lvm::pool::PoolService as LvmSvc,
    lvs::pool::PoolService as LvsSvc,
};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

/// Probe for pools using this criteria.
#[derive(Debug, Clone)]
pub enum PoolProbe {
    Uuid(String),
    UuidOrName(String),
    NameUuid { name: String, uuid: Option<String> },
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
                source: Errno::EINVAL,
                msg: "invalid argument, missing devices".to_string(),
            });
        }

        let backend = PoolType::try_from(args.pooltype).map_err(|_| {
            LvsError::Invalid {
                source: Errno::EINVAL,
                msg: format!("invalid pooltype provided: {}", args.pooltype),
            }
        })?;
        if backend == PoolType::Lvs {
            if let Some(s) = args.uuid.clone() {
                let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| {
                    LvsError::Invalid {
                        source: Errno::EINVAL,
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
            backend: backend.into(),
        })
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

impl TryFrom<ImportPoolRequest> for PoolArgs {
    type Error = LvsError;
    fn try_from(args: ImportPoolRequest) -> Result<Self, Self::Error> {
        if args.disks.is_empty() {
            return Err(LvsError::Invalid {
                source: Errno::EINVAL,
                msg: "invalid argument, missing devices".to_string(),
            });
        }

        let backend = PoolType::try_from(args.pooltype).map_err(|_| {
            LvsError::Invalid {
                source: Errno::EINVAL,
                msg: format!("invalid pooltype provided: {}", args.pooltype),
            }
        })?;
        if backend == PoolType::Lvs {
            if let Some(s) = args.uuid.clone() {
                let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| {
                    LvsError::Invalid {
                        source: Errno::EINVAL,
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
            backend: backend.into(),
        })
    }
}

impl Default for PoolService {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolService {
    pub fn new() -> Self {
        Self {
            name: String::from("PoolSvc"),
            client_context: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
    /// Return a backend for the given type.
    fn backend(
        &self,
        pooltype: i32,
    ) -> Result<Box<dyn PoolRpc>, tonic::Status> {
        Ok(match PoolBackend::try_from(pooltype)? {
            PoolBackend::Lvs => Box::new(LvsSvc::new()),
            PoolBackend::Lvm => Box::new(LvmSvc::new()),
        })
    }
    /// Probe backends for the given name and/or uuid and return the right one.
    async fn probe_backend(
        &self,
        name: &str,
        uuid: &Option<String>,
    ) -> Result<Box<dyn PoolRpc>, tonic::Status> {
        let probe = PoolProbe::NameUuid {
            name: name.to_owned(),
            uuid: uuid.to_owned(),
        };
        Ok(match self.probe_backend_kind(probe).await? {
            PoolBackend::Lvm => Box::new(LvmSvc::new()),
            PoolBackend::Lvs => Box::new(LvsSvc::new()),
        })
    }

    pub(crate) async fn probe_backend_kind(
        &self,
        probe: PoolProbe,
    ) -> Result<PoolBackend, tonic::Status> {
        match (
            LvmSvc::probe(&probe).await,
            LvsSvc::probe(probe.clone()).await,
        ) {
            (Ok(true), _) => Ok(PoolBackend::Lvm),
            (_, Ok(true)) => Ok(PoolBackend::Lvs),
            (Err(error), _) | (_, Err(error)) => Err(error),
            _ => Err(Status::not_found(format!("Pool {probe:?} not found"))),
        }
    }
}

#[tonic::async_trait]
impl PoolRpc for PoolService {
    #[named]
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        let backend = self.backend(request.get_ref().pooltype)?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.create_pool(request).await
            },
        )
        .await
    }

    #[named]
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<()> {
        let backend = self
            .probe_backend(&request.get_ref().name, &request.get_ref().uuid)
            .await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.destroy_pool(request).await
            },
        )
        .await
    }

    #[named]
    async fn export_pool(
        &self,
        request: Request<ExportPoolRequest>,
    ) -> GrpcResult<()> {
        let backend = self
            .probe_backend(&request.get_ref().name, &request.get_ref().uuid)
            .await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.export_pool(request).await
            },
        )
        .await
    }

    #[named]
    async fn import_pool(
        &self,
        request: Request<ImportPoolRequest>,
    ) -> GrpcResult<Pool> {
        let backend = self.backend(request.get_ref().pooltype)?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.import_pool(request).await
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
                let args = request.into_inner();

                // todo: what is the intent here when None, to only return pools
                //  of Lvs?
                // todo: Also, what todo when we hit an error listing any of the
                //  types? Or should we have separate lists per type?
                let pool_type = args.pooltype.as_ref().map(|v| v.value);
                let pool_type = match pool_type {
                    None => None,
                    Some(pool_type) => {
                        Some(PoolType::try_from(pool_type).map_err(|_| {
                            Status::invalid_argument("Unknown pool type")
                        })?)
                    }
                };

                let lvm = matches!(pool_type, None | Some(PoolType::Lvm));
                let lvs = matches!(pool_type, None | Some(PoolType::Lvs));

                let mut pools = vec![];
                if lvm {
                    pools.extend(
                        match LvmSvc::new().list_svc_pools(&args).await {
                            Ok(pools) => Ok(pools),
                            Err(mut status) => {
                                status.metadata_mut().insert(
                                    "lvm",
                                    tonic::metadata::MetadataValue::from(0),
                                );
                                Err(status)
                            }
                        }?,
                    );
                }

                if lvs {
                    pools.extend(
                        match LvsSvc::new().list_svc_pools(&args).await {
                            Ok(pools) => Ok(pools),
                            Err(mut status) => {
                                status.metadata_mut().insert(
                                    "lvs",
                                    tonic::metadata::MetadataValue::from(0),
                                );
                                Err(status)
                            }
                        }?,
                    );
                }

                Ok(Response::new(ListPoolsResponse {
                    pools,
                }))
            },
        )
        .await
    }
}
