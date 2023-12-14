use crate::{
    grpc::{rpc_submit, GrpcClientContext, GrpcResult, Serializer},
    lvs::Lvs,
};
use futures::FutureExt;
use io_engine_api::v1::stats::*;
use std::fmt::Debug;
use tonic::{Request, Response, Status};

use crate::core::{BlockDeviceIoStats, CoreError, UntypedBdev};
use ::function_name::named;
use std::panic::AssertUnwindSafe;

/// RPC service for Resource IoStats.
#[derive(Debug)]
#[allow(dead_code)]
pub struct StatsService {
    name: String,
    client_context: tokio::sync::Mutex<Option<GrpcClientContext>>,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for StatsService
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

impl Default for StatsService {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsService {
    /// Constructor for Stats Service.
    pub fn new() -> Self {
        Self {
            name: String::from("StatsSvc"),
            client_context: tokio::sync::Mutex::new(None),
        }
    }
}

#[tonic::async_trait]
impl StatsRpc for StatsService {
    #[named]
    async fn get_pool_io_stats(
        &self,
        request: Request<ListStatsOption>,
    ) -> GrpcResult<PoolIoStatsResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit::<_, _, CoreError>(async move {
                    let mut pool_stats: Vec<IoStats> = Vec::new();
                    if let Some(name) = args.name {
                        if let Some(l) = Lvs::lookup(&name) {
                            let stats = l.base_bdev().stats_async().await?;
                            let io_stat = IoStats::from(ExternalType((
                                name.clone(),
                                stats,
                            )));
                            pool_stats.push(io_stat);
                        }
                    } else {
                        let bdev_list: Vec<(String, UntypedBdev)> = Lvs::iter()
                            .map(|lvs| {
                                (lvs.name().to_string(), lvs.base_bdev())
                            })
                            .collect();
                        for (name, bdev) in bdev_list.iter() {
                            let bdev_stat = bdev.stats_async().await?;
                            let io_stat = IoStats::from(ExternalType((
                                name.clone(),
                                bdev_stat,
                            )));
                            pool_stats.push(io_stat);
                        }
                    }
                    Ok(PoolIoStatsResponse {
                        stats: pool_stats,
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
    async fn reset_io_stats(&self, _request: Request<()>) -> GrpcResult<()> {
        unimplemented!()
    }
}

struct ExternalType<T>(T);

/// Conversion fn to get gRPC type IOStat from BlockDeviceIoStats.
impl From<ExternalType<(String, BlockDeviceIoStats)>> for IoStats {
    fn from(value: ExternalType<(String, BlockDeviceIoStats)>) -> Self {
        Self {
            name: value.0 .0,
            num_read_ops: value.0 .1.num_read_ops,
            bytes_read: value.0 .1.bytes_read,
            num_write_ops: value.0 .1.num_write_ops,
            bytes_written: value.0 .1.bytes_written,
            num_unmap_ops: value.0 .1.num_unmap_ops,
            bytes_unmapped: value.0 .1.bytes_unmapped,
            read_latency_ticks: value.0 .1.read_latency_ticks,
            write_latency_ticks: value.0 .1.write_latency_ticks,
            unmap_latency_ticks: value.0 .1.unmap_latency_ticks,
            max_read_latency_ticks: value.0 .1.max_read_latency_ticks,
            min_read_latency_ticks: value.0 .1.min_read_latency_ticks,
            max_write_latency_ticks: value.0 .1.max_write_latency_ticks,
            min_write_latency_ticks: value.0 .1.min_write_latency_ticks,
            max_unmap_latency_ticks: value.0 .1.max_unmap_latency_ticks,
            min_unmap_latency_ticks: value.0 .1.min_unmap_latency_ticks,
            tick_rate: value.0 .1.tick_rate,
        }
    }
}
