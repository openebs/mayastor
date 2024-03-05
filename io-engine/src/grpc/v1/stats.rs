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
    lvs::Lvs,
};
use futures::{future::join_all, FutureExt};
use io_engine_api::v1::stats::*;
use std::{convert::TryFrom, fmt::Debug, panic::AssertUnwindSafe};
use tonic::{Request, Response, Status};

use crate::{
    bdev::{nexus, Nexus},
    core::{BlockDeviceIoStats, CoreError, LogicalVolume, UntypedBdev},
    lvs::{Lvol, LvsLvol},
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
            match lock_manager.lock(Some(ctx.timeout)).await {
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
            match lock_manager.lock(Some(ctx.timeout)).await {
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
            let rx = rpc_submit::<_, _, CoreError>(async move {
                let pool_stats_future: Vec<_> = if let Some(name) = args.name {
                    if let Some(l) = Lvs::lookup(&name) {
                        vec![get_stats(name, l.uuid(), l.base_bdev())]
                    } else {
                        vec![]
                    }
                } else {
                    Lvs::iter()
                        .map(|lvs| {
                            get_stats(
                                lvs.name().to_string(),
                                lvs.uuid(),
                                lvs.base_bdev(),
                            )
                        })
                        .collect()
                };

                let pool_stats: Result<Vec<_>, _> =
                    join_all(pool_stats_future).await.into_iter().collect();
                let pool_stats = pool_stats?;
                Ok(PoolIoStatsResponse {
                    stats: pool_stats,
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
    async fn get_nexus_io_stats(
        &self,
        request: Request<ListStatsOption>,
    ) -> GrpcResult<NexusIoStatsResponse> {
        self.nexus_lock(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit::<_, _, CoreError>(async move {
                    let nexus_stats_future: Vec<_> =
                        if let Some(name) = args.name {
                            if let Some(nexus) = nexus::nexus_lookup(&name) {
                                vec![nexus_stats(
                                    nexus.name.clone(),
                                    nexus.uuid().to_string(),
                                    nexus,
                                )]
                            } else {
                                vec![]
                            }
                        } else {
                            nexus::nexus_iter()
                                .map(|nexus| {
                                    nexus_stats(
                                        nexus.name.clone(),
                                        nexus.uuid().to_string(),
                                        nexus,
                                    )
                                })
                                .collect()
                        };
                    let nexus_stats: Result<Vec<_>, _> =
                        join_all(nexus_stats_future)
                            .await
                            .into_iter()
                            .collect();
                    let nexus_stats = nexus_stats?;
                    Ok(NexusIoStatsResponse {
                        stats: nexus_stats,
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

    async fn get_replica_io_stats(
        &self,
        request: Request<ListStatsOption>,
    ) -> GrpcResult<ReplicaIoStatsResponse> {
        self.shared(self.replica_svc.rw_lock().await, async move {
            let args = request.into_inner();
            let rx = rpc_submit::<_, _, CoreError>(async move {
                let replica_stats_future: Vec<_> = if let Some(name) = args.name
                {
                    UntypedBdev::bdev_first()
                        .and_then(|bdev| {
                            bdev.into_iter().find(|b| {
                                b.driver() == "lvol" && b.name() == name
                            })
                        })
                        .and_then(|b| Lvol::try_from(b).ok())
                        .map(|lvol| vec![replica_stats(lvol)])
                        .unwrap_or_default()
                } else {
                    let mut lvols = Vec::new();
                    if let Some(bdev) = UntypedBdev::bdev_first() {
                        lvols = bdev
                            .into_iter()
                            .filter(|b| b.driver() == "lvol")
                            .map(|b| Lvol::try_from(b).unwrap())
                            .collect();
                    }
                    lvols.into_iter().map(replica_stats).collect()
                };
                let replica_stats: Result<Vec<_>, _> =
                    join_all(replica_stats_future).await.into_iter().collect();
                let replica_stats = replica_stats?;
                Ok(ReplicaIoStatsResponse {
                    stats: replica_stats,
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

struct ExternalType<T>(T);

/// Conversion fn to get gRPC type IOStat from BlockDeviceIoStats.
impl From<ExternalType<(String, String, BlockDeviceIoStats)>> for IoStats {
    fn from(value: ExternalType<(String, String, BlockDeviceIoStats)>) -> Self {
        let stats = value.0 .2;
        Self {
            name: value.0 .0,
            uuid: value.0 .1,
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

/// Returns IoStats for a given BlockDevice.
async fn get_stats(
    name: String,
    uuid: String,
    bdev: UntypedBdev,
) -> Result<IoStats, CoreError> {
    let stats = bdev.stats_async().await?;
    Ok(IoStats::from(ExternalType((name, uuid, stats))))
}

/// Returns IoStats for a given Lvol(Replica).
async fn replica_stats(lvol: Lvol) -> Result<ReplicaIoStats, CoreError> {
    let stats = lvol.as_bdev().stats_async().await?;
    let io_stat =
        IoStats::from(ExternalType((lvol.name(), lvol.uuid(), stats)));
    let replica_stat = ReplicaIoStats {
        stats: Some(io_stat),
        entity_id: lvol.entity_id(),
    };
    Ok(replica_stat)
}

/// Returns IoStats for a given Nexus.
async fn nexus_stats(
    name: String,
    uuid: String,
    bdev: &Nexus<'_>,
) -> Result<IoStats, CoreError> {
    let stats = bdev.bdev_stats().await?;
    Ok(IoStats::from(ExternalType((name, uuid, stats))))
}
