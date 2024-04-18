use crate::{
    core::{logical_volume::LvolSpaceUsage, Protocol},
    grpc::{
        v1::pool::PoolProbe,
        GrpcClientContext,
        GrpcResult,
        RWLock,
        RWSerializer,
    },
    pool_backend::PoolBackend,
};
use ::function_name::named;
use futures::FutureExt;
use io_engine_api::v1::{pool::PoolType, replica::*};
use std::{convert::TryFrom, panic::AssertUnwindSafe};
use tonic::{Request, Response, Status};

use super::{
    lvm::replica::ReplicaService as LvmSvc,
    lvs::replica::ReplicaService as LvsSvc,
};

#[derive(Debug, Clone)]
pub struct ReplicaService {
    #[allow(unused)]
    name: String,
    client_context:
        std::sync::Arc<tokio::sync::RwLock<Option<GrpcClientContext>>>,
    pool_svc: super::pool::PoolService,
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

impl ReplicaService {
    pub fn new(pool_svc: super::pool::PoolService) -> Self {
        Self {
            name: String::from("ReplicaSvc"),
            client_context: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            pool_svc,
        }
    }

    /// Probe backends for the given replica uuid and return the right one.
    async fn probe_backend(
        &self,
        replica_uuid: &str,
    ) -> Result<Box<dyn ReplicaRpc>, tonic::Status> {
        Ok(match self.probe_backend_kind(replica_uuid).await? {
            PoolBackend::Lvs => Box::new(LvsSvc::new()),
            PoolBackend::Lvm => Box::new(LvmSvc::new()),
        })
    }
    /// Probe backends for the given pool uuid and return the right one.
    pub async fn probe_backend_pool(
        &self,
        probe: PoolProbe,
    ) -> Result<Box<dyn ReplicaRpc>, tonic::Status> {
        Ok(match self.pool_svc.probe_backend_kind(probe).await? {
            PoolBackend::Lvs => Box::new(LvsSvc::new()),
            PoolBackend::Lvm => Box::new(LvmSvc::new()),
        })
    }
    async fn probe_backend_kind(
        &self,
        uuid: &str,
    ) -> Result<PoolBackend, tonic::Status> {
        match (LvsSvc::probe(uuid).await, LvmSvc::probe(uuid).await) {
            (Ok(true), _) => Ok(PoolBackend::Lvs),
            (_, Ok(true)) => Ok(PoolBackend::Lvm),
            (Err(error), _) | (_, Err(error)) => Err(error),
            _ => Err(Status::not_found(format!("Replica {uuid} not found"))),
        }
    }
}
pub(crate) fn filter_replicas_by_replica_type(
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

#[tonic::async_trait]
impl ReplicaRpc for ReplicaService {
    #[named]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let probe =
            PoolProbe::UuidOrName(request.get_ref().pooluuid.to_owned());
        let backend = self.probe_backend_pool(probe).await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
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

                backend.create_replica(request).await
            },
        )
        .await
    }

    #[named]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<()> {
        let backend = self.probe_backend(&request.get_ref().uuid).await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.destroy_replica(request).await
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
            let args = request.into_inner();
            trace!("{:?}", args);

            let mut replicas = vec![];

            if args.pooltypes.is_empty()
                || args.pooltypes.iter().any(|t| *t == PoolType::Lvs as i32)
            {
                replicas.extend(LvsSvc::new().list_lvs_replicas().await?);
            }
            if args.pooltypes.iter().any(|t| *t == PoolType::Lvm as i32) {
                replicas.extend(LvmSvc::new().list_lvm_replicas(&args).await?);
            }

            let retain = |arg: Option<&String>, val: &String| -> bool {
                arg.is_none() || arg == Some(val)
            };

            replicas.retain(|replica| {
                retain(args.poolname.as_ref(), &replica.poolname)
                    && retain(args.pooluuid.as_ref(), &replica.pooluuid)
                    && retain(args.name.as_ref(), &replica.name)
                    && retain(args.uuid.as_ref(), &replica.uuid)
            });

            Ok(Response::new(ListReplicasResponse {
                replicas: filter_replicas_by_replica_type(replicas, args.query),
            }))
        })
        .await
    }

    #[named]
    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let backend = self.probe_backend(&request.get_ref().uuid).await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.share_replica(request).await
            },
        )
        .await
    }

    #[named]
    async fn unshare_replica(
        &self,
        request: Request<UnshareReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let backend = self.probe_backend(&request.get_ref().uuid).await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.unshare_replica(request).await
            },
        )
        .await
    }

    #[named]
    async fn resize_replica(
        &self,
        request: Request<ResizeReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let backend = self.probe_backend(&request.get_ref().uuid).await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.resize_replica(request).await
            },
        )
        .await
    }

    #[named]
    async fn set_replica_entity_id(
        &self,
        request: Request<SetReplicaEntityIdRequest>,
    ) -> GrpcResult<Replica> {
        let backend = self.probe_backend(&request.get_ref().uuid).await?;
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                info!("{:?}", request.get_ref());

                backend.set_replica_entity_id(request).await
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
