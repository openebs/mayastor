use crate::{
    core::Share,
    grpc::{rpc_submit, GrpcClientContext, GrpcResult, Serializer},
    lvs::{Error as LvsError, Lvs},
    pool::{PoolArgs, PoolBackend},
};
use futures::FutureExt;
use nix::errno::Errno;
use std::{convert::TryFrom, fmt::Debug};
use tonic::{Request, Response, Status};

use rpc::mayastor::v1::pool::*;

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

use ::function_name::named;
use std::panic::AssertUnwindSafe;

/// RPC service for mayastor pool operations
#[derive(Debug)]
#[allow(dead_code)]
pub struct PoolService {
    name: String,
    client_context: tokio::sync::Mutex<Option<GrpcClientContext>>,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for PoolService
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

impl TryFrom<CreatePoolRequest> for PoolArgs {
    type Error = LvsError;
    fn try_from(args: CreatePoolRequest) -> Result<Self, Self::Error> {
        if args.disks.is_empty() {
            return Err(LvsError::Invalid {
                source: Errno::EINVAL,
                msg: "invalid argument, missing devices".to_string(),
            });
        }

        if let Some(s) = args.uuid.clone() {
            let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| {
                LvsError::Invalid {
                    source: Errno::EINVAL,
                    msg: format!("invalid uuid provided, {}", e),
                }
            })?;
        }

        Ok(Self {
            name: args.name,
            disks: args.disks,
            uuid: args.uuid,
        })
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

        if let Some(s) = args.uuid.clone() {
            let _uuid = uuid::Uuid::parse_str(s.as_str()).map_err(|e| {
                LvsError::Invalid {
                    source: Errno::EINVAL,
                    msg: format!("invalid uuid provided, {}", e),
                }
            })?;
        }

        Ok(Self {
            name: args.name,
            disks: args.disks,
            uuid: args.uuid,
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
            client_context: tokio::sync::Mutex::new(None),
        }
    }
}

impl From<Lvs> for Pool {
    fn from(l: Lvs) -> Self {
        Self {
            uuid: l.uuid(),
            name: l.name().into(),
            disks: vec![l.base_bdev().bdev_uri().unwrap_or_else(|| "".into())],
            state: PoolState::PoolOnline.into(),
            capacity: l.capacity(),
            used: l.used(),
            pooltype: PoolType::Lvs as i32,
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
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                match PoolBackend::try_from(args.pooltype)? {
                    PoolBackend::Lvs => {
                        let rx = rpc_submit::<_, _, LvsError>(async move {
                            let pool = Lvs::create_or_import(
                                PoolArgs::try_from(args)?,
                            )
                            .await?;
                            Ok(Pool::from(pool))
                        })?;

                        rx.await
                            .map_err(|_| Status::cancelled("cancelled"))?
                            .map_err(Status::from)
                            .map(Response::new)
                    }
                }
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
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, LvsError>(async move {
                    if let Some(pool) = Lvs::lookup(&args.name) {
                        if args.uuid.is_some() && args.uuid != Some(pool.uuid())
                        {
                            return Err(LvsError::Invalid {
                                source: Errno::EINVAL,
                                msg: format!(
                                    "invalid uuid {}, found pool with uuid {}",
                                    args.uuid.unwrap(),
                                    pool.uuid(),
                                ),
                            });
                        }
                        pool.destroy().await?;
                    } else {
                        return Err(LvsError::Invalid {
                            source: Errno::EINVAL,
                            msg: format!("pool {} not found", args.name,),
                        });
                    }
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
    async fn export_pool(
        &self,
        request: Request<ExportPoolRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, LvsError>(async move {
                    if let Some(pool) = Lvs::lookup(&args.name) {
                        if args.uuid.is_some() && args.uuid != Some(pool.uuid())
                        {
                            return Err(LvsError::Invalid {
                                source: Errno::EINVAL,
                                msg: format!(
                                    "invalid uuid {}, found pool with uuid {}",
                                    args.uuid.unwrap(),
                                    pool.uuid(),
                                ),
                            });
                        }
                        pool.export().await?;
                    } else {
                        return Err(LvsError::Invalid {
                            source: Errno::EINVAL,
                            msg: format!("pool {} not found", args.name),
                        });
                    }
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
    async fn import_pool(
        &self,
        request: Request<ImportPoolRequest>,
    ) -> GrpcResult<Pool> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, LvsError>(async move {
                    let pool = Lvs::import_from_args(PoolArgs::try_from(args)?)
                        .await?;
                    Ok(Pool::from(pool))
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
    async fn list_pools(
        &self,
        request: Request<ListPoolOptions>,
    ) -> GrpcResult<ListPoolsResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, LvsError>(async move {
                    let mut pools = Vec::new();
                    let args = request.into_inner();
                    if let Some(name) = args.name {
                        if let Some(l) = Lvs::lookup(&name) {
                            pools.push(l.into())
                        };
                    } else {
                        Lvs::iter().for_each(|l| pools.push(l.into()));
                    }
                    Ok(ListPoolsResponse {
                        pools,
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
}
