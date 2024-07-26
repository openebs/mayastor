use crate::{
    core::lock::ResourceLockManager,
    grpc::{
        rpc_submit,
        v1::{pool::PoolService, replica::ReplicaService},
        GrpcClientContext,
        GrpcResult,
        RWLock,
        Serializer,
    },
};
use futures::{future::join_all, FutureExt};
use io_engine_api::v1::stats::*;
use std::{fmt::Debug, panic::AssertUnwindSafe};
use tonic::{Request, Response, Status};

use crate::{
    bdev::nexus,
    core::{BdevStater, BdevStats, CoreError, UntypedBdev},
    grpc::v1::{pool::GrpcPoolFactory, replica::GrpcReplicaFactory},
    pool_backend::ListPoolArgs,
    replica_backend::{ListReplicaArgs, ReplicaBdevStats},
};
use ::function_name::named;

/// RPC service for Resource IoStats.
#[derive(Debug)]
#[allow(dead_code)]
pub struct StatsService {
    name: String,
    client_context:
        std::sync::Arc<tokio::sync::RwLock<Option<GrpcClientContext>>>,
    pool_svc: PoolService,
    replica_svc: ReplicaService,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for StatsService
where
    T: Send + 'static,
    F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
{
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        // Taking write lock of Stats service. This will hold off read ops.
        let _statsvc_lock = self.client_context.write().await;

        // Takes read lock of Pool and Replica service.
        let _pool_context = self.pool_svc.rw_lock().await.read().await;
        let _replica_context = self.replica_svc.rw_lock().await.read().await;

        let lock_manager = ResourceLockManager::get_instance();
        // For nexus global lock.
        let _global_guard =
            match lock_manager.lock(Some(ctx.timeout), false).await {
                Some(g) => Some(g),
                None => return Err(Status::deadline_exceeded(
                    "Failed to acquire access to object within given timeout",
                )),
            };
        let fut = AssertUnwindSafe(f).catch_unwind();
        let r = fut.await;
        r.unwrap_or_else(|_| {
            warn!("{}: gRPC method panicked, args: {}", ctx.id, ctx.args);
            Err(Status::cancelled("gRPC method panicked".to_string()))
        })
    }
}

impl StatsService {
    async fn shared<
        T: Send + 'static,
        F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
    >(
        &self,
        reader: &tokio::sync::RwLock<Option<GrpcClientContext>>,
        f: F,
    ) -> Result<T, Status> {
        let _stat_svc = self.client_context.read().await;
        let _svc_lock = reader.read().await;
        let fut = AssertUnwindSafe(f).catch_unwind();
        let r = fut.await;
        r.unwrap_or_else(|_| {
            warn!("gRPC method panicked");
            Err(Status::cancelled("gRPC method panicked".to_string()))
        })
    }

    async fn nexus_lock<
        T: Send + 'static,
        F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
    >(
        &self,
        ctx: GrpcClientContext,
        f: F,
    ) -> Result<T, Status> {
        let _stat_svc = self.client_context.read().await;
        let lock_manager = ResourceLockManager::get_instance();
        // For nexus global lock.
        let _global_guard =
            match lock_manager.lock(Some(ctx.timeout), false).await {
                Some(g) => Some(g),
                None => return Err(Status::deadline_exceeded(
                    "Failed to acquire access to object within given timeout",
                )),
            };
        let fut = AssertUnwindSafe(f).catch_unwind();
        let r = fut.await;
        r.unwrap_or_else(|_| {
            warn!("gRPC method panicked, args");
            Err(Status::cancelled("gRPC method panicked".to_string()))
        })
    }
}

impl StatsService {
    /// Constructor for Stats Service.
    pub fn new(pool_svc: PoolService, replica_svc: ReplicaService) -> Self {
        Self {
            name: String::from("StatsSvc"),
            client_context: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            pool_svc,
            replica_svc,
        }
    }
}

#[tonic::async_trait]
impl StatsRpc for StatsService {
    async fn get_pool_io_stats(
        &self,
        request: Request<ListStatsOption>,
    ) -> GrpcResult<PoolIoStatsResponse> {
        self.shared(self.pool_svc.rw_lock().await, async move {
            let args = request.into_inner();
            crate::spdk_submit!(async move {
                let mut pools = vec![];
                let args = ListPoolArgs::new_named(args.name);
                for factory in GrpcPoolFactory::factories() {
                    pools.extend(
                        factory.list_ops(&args).await.unwrap_or_default(),
                    );
                }
                let pools_stats_future = pools.iter().map(|r| r.stats());
                let pools_stats =
                    join_all(pools_stats_future).await.into_iter();
                let stats = pools_stats
                    .map(|d| d.map(Into::into))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(PoolIoStatsResponse {
                    stats,
                })
            })
        })
        .await
    }

    #[named]
    async fn get_nexus_io_stats(
        &self,
        request: Request<ListStatsOption>,
    ) -> GrpcResult<NexusIoStatsResponse> {
        self.nexus_lock(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                crate::spdk_submit!(async move {
                    let nexus_stats_future = if let Some(name) = args.name {
                        let nexus = nexus::nexus_lookup(&name)
                            .ok_or(Status::not_found("Nexus not found"))?;
                        vec![nexus.stats()]
                    } else {
                        nexus::nexus_iter().map(|nexus| nexus.stats()).collect()
                    };
                    let nexus_stats = join_all(nexus_stats_future)
                        .await
                        .into_iter()
                        .map(|d| d.map(Into::into));
                    let stats = nexus_stats.collect::<Result<Vec<_>, _>>()?;
                    Ok(NexusIoStatsResponse {
                        stats,
                    })
                })
            },
        )
        .await
    }
    async fn get_replica_io_stats(
        &self,
        request: Request<ListStatsOption>,
    ) -> GrpcResult<ReplicaIoStatsResponse> {
        self.shared(self.replica_svc.rw_lock().await, async move {
            let args = request.into_inner();
            crate::spdk_submit!(async move {
                let mut replicas = vec![];
                let args = ListReplicaArgs::new_named(args.name);
                for factory in GrpcReplicaFactory::factories() {
                    replicas.extend(
                        factory.list_ops(&args).await.unwrap_or_default(),
                    );
                }
                let replica_stats_future = replicas.iter().map(|r| r.stats());
                let replica_stats =
                    join_all(replica_stats_future).await.into_iter();
                let stats = replica_stats
                    .map(|d| d.map(Into::into))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ReplicaIoStatsResponse {
                    stats,
                })
            })
        })
        .await
    }

    #[named]
    async fn reset_io_stats(&self, request: Request<()>) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, CoreError>(async move {
                    if let Some(bdev) = UntypedBdev::bdev_first() {
                        for bdev in bdev.into_iter() {
                            let _ = bdev.reset_bdev_io_stats().await?;
                        }
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
}

/// Conversion fn to get gRPC type IOStat from BlockDeviceIoStats.
impl From<BdevStats> for IoStats {
    fn from(value: BdevStats) -> Self {
        let stats = value.stats;
        Self {
            name: value.name,
            uuid: value.uuid,
            num_read_ops: stats.num_read_ops,
            bytes_read: stats.bytes_read,
            num_write_ops: stats.num_write_ops,
            bytes_written: stats.bytes_written,
            num_unmap_ops: stats.num_unmap_ops,
            bytes_unmapped: stats.bytes_unmapped,
            read_latency_ticks: stats.read_latency_ticks,
            write_latency_ticks: stats.write_latency_ticks,
            unmap_latency_ticks: stats.unmap_latency_ticks,
            max_read_latency_ticks: stats.max_read_latency_ticks,
            min_read_latency_ticks: stats.min_read_latency_ticks,
            max_write_latency_ticks: stats.max_write_latency_ticks,
            min_write_latency_ticks: stats.min_write_latency_ticks,
            max_unmap_latency_ticks: stats.max_unmap_latency_ticks,
            min_unmap_latency_ticks: stats.min_unmap_latency_ticks,
            tick_rate: stats.tick_rate,
        }
    }
}
/// Conversion fn to get gRPC type IOStat from BlockDeviceIoStats.
impl From<ReplicaBdevStats> for ReplicaIoStats {
    fn from(value: ReplicaBdevStats) -> Self {
        Self {
            entity_id: value.entity_id,
            stats: Some(value.stats.into()),
        }
    }
}
