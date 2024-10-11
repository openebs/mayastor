use crate::{
    grpc::GrpcResult,
    rebuild::{
        RebuildError,
        RebuildState,
        RebuildStats,
        SnapshotRebuildError,
        SnapshotRebuildJob,
    },
};
use io_engine_api::v1::{
    snapshot_rebuild,
    snapshot_rebuild::{
        CreateSnapshotRebuildRequest,
        DestroySnapshotRebuildRequest,
        ListSnapshotRebuildRequest,
        ListSnapshotRebuildResponse,
        SnapshotRebuild,
        SnapshotRebuildRpc,
    },
};
use std::sync::Arc;
use tonic::Request;

#[derive(Debug)]
pub struct SnapshotRebuildService {
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    replica_svc: super::replica::ReplicaService,
}

impl SnapshotRebuildService {
    pub fn new(replica_svc: super::replica::ReplicaService) -> Self {
        Self {
            name: String::from("SnapshotRebuildService"),
            replica_svc,
        }
    }
}

#[tonic::async_trait]
impl SnapshotRebuildRpc for SnapshotRebuildService {
    async fn create_snapshot_rebuild(
        &self,
        request: Request<CreateSnapshotRebuildRequest>,
    ) -> GrpcResult<SnapshotRebuild> {
        let request = request.into_inner();

        crate::spdk_submit!(async move {
            info!("{:?}", request);

            let None = request.bitmap else {
                return Err(tonic::Status::invalid_argument(
                    "BitMap not supported",
                ));
            };
            if let Ok(job) = SnapshotRebuildJob::lookup(&request.uuid) {
                return Ok(SnapshotRebuild::from(SnapRebuild::from(job).await));
            }
            SnapshotRebuildJob::builder()
                .with_uuid(&request.uuid)
                .with_replica_uuid(&request.replica_uuid)
                .with_snapshot_uuid(&request.snapshot_uuid)
                .with_replica_uri(request.replica_uri)
                .with_snapshot_uri(request.snapshot_uri)
                .build()
                .await?
                .store()?;

            let job = SnapRebuild::lookup(&request.uuid).await?;
            job.start().await?;
            Ok(SnapshotRebuild::from(job))
        })
    }
    async fn list_snapshot_rebuild(
        &self,
        request: Request<ListSnapshotRebuildRequest>,
    ) -> GrpcResult<ListSnapshotRebuildResponse> {
        crate::spdk_submit!(async move {
            let args = request.into_inner();
            trace!("{:?}", args);
            match args.replica_uuid {
                None => {
                    let jobs = SnapshotRebuildJob::list();
                    let mut rebuilds = Vec::with_capacity(jobs.len());
                    for job in jobs {
                        rebuilds.push(SnapRebuild::from(job).await.into());
                    }
                    Ok(ListSnapshotRebuildResponse {
                        rebuilds,
                    })
                }
                Some(uuid) => {
                    let job = SnapRebuild::lookup(&uuid).await?;
                    Ok(ListSnapshotRebuildResponse {
                        rebuilds: vec![job.into()],
                    })
                }
            }
        })
    }
    async fn destroy_snapshot_rebuild(
        &self,
        request: Request<DestroySnapshotRebuildRequest>,
    ) -> GrpcResult<()> {
        crate::spdk_submit!(async move {
            let args = request.into_inner();
            info!("{:?}", args);
            let Ok(job) = SnapshotRebuildJob::lookup(&args.uuid) else {
                return Err(tonic::Status::not_found(""));
            };
            let rx = match job.force_stop() {
                either::Either::Left(chan) => chan.await,
                either::Either::Right(stopped) => Ok(stopped),
            };
            info!("Snapshot Rebuild stopped: {rx:?}");
            job.destroy();
            Ok(())
        })
    }
}

struct SnapRebuild {
    stats: RebuildStats,
    job: Arc<SnapshotRebuildJob>,
}
impl SnapRebuild {
    async fn from(job: Arc<SnapshotRebuildJob>) -> Self {
        let stats = job.stats().await;
        Self {
            stats,
            job,
        }
    }
    async fn lookup(uuid: &str) -> Result<Self, tonic::Status> {
        let job = SnapshotRebuildJob::lookup(uuid)?;
        Ok(Self::from(job).await)
    }
    async fn start(&self) -> Result<(), tonic::Status> {
        let _receiver = self.job.start().await?;
        Ok(())
    }
}

impl From<SnapRebuild> for SnapshotRebuild {
    fn from(value: SnapRebuild) -> Self {
        let stats = value.stats;
        let job = value.job;
        Self {
            uuid: job.uuid().to_string(),
            replica_uuid: job.replica_uuid().to_string(),
            snapshot_uuid: job.snapshot_uuid().to_string(),
            replica_uri: job.replica_uri().to_string(),
            snapshot_uri: job.snapshot_uri().to_string(),
            status: snapshot_rebuild::RebuildStatus::from(job.state()) as i32,
            total: stats.blocks_total * stats.block_size,
            rebuilt: stats.blocks_transferred * stats.block_size,
            remaining: stats.blocks_remaining * stats.block_size,
            persisted_checkpoint: 0,
            start_timestamp: Some(stats.start_time.into()),
            end_timestamp: stats.end_time.map(Into::into),
            target_remote: false,
        }
    }
}

impl From<RebuildState> for snapshot_rebuild::RebuildStatus {
    fn from(value: RebuildState) -> Self {
        use snapshot_rebuild::RebuildStatus;
        match value {
            RebuildState::Init => RebuildStatus::Created,
            RebuildState::Running => RebuildStatus::Running,
            RebuildState::Stopped => RebuildStatus::Failed,
            RebuildState::Paused => RebuildStatus::Paused,
            RebuildState::Failed => RebuildStatus::Failed,
            RebuildState::Completed => RebuildStatus::Successful,
        }
    }
}

impl From<RebuildError> for tonic::Status {
    fn from(value: RebuildError) -> Self {
        let message = value.to_string();
        match value {
            RebuildError::JobAlreadyExists {
                ..
            } => tonic::Status::already_exists(message),
            RebuildError::NoCopyBuffer {
                ..
            } => tonic::Status::internal(message),
            RebuildError::InvalidSrcDstRange {
                ..
            } => tonic::Status::out_of_range(message),
            RebuildError::InvalidMapRange {
                ..
            } => tonic::Status::out_of_range(message),
            RebuildError::SameBdev {
                ..
            } => tonic::Status::invalid_argument(message),
            RebuildError::NoBdevHandle {
                ..
            } => tonic::Status::failed_precondition(message),
            RebuildError::BdevNotFound {
                ..
            } => tonic::Status::failed_precondition(message),
            RebuildError::JobNotFound {
                ..
            } => tonic::Status::not_found(message),
            RebuildError::BdevInvalidUri {
                ..
            } => tonic::Status::invalid_argument(message),
            RebuildError::RebuildTasksChannel {
                ..
            } => tonic::Status::resource_exhausted(message),
            RebuildError::SnapshotRebuild {
                source,
            } => match source {
                SnapshotRebuildError::LocalBdevNotFound {
                    ..
                } => tonic::Status::not_found(message),
                SnapshotRebuildError::RemoteNoUri {
                    ..
                } => tonic::Status::internal(message),
                SnapshotRebuildError::NotAReplica {
                    ..
                } => tonic::Status::invalid_argument(message),
                // todo better error check here, what if bdev uri is invalid?
                SnapshotRebuildError::UriBdevOpen {
                    ..
                } => tonic::Status::not_found(message),
            },
            _ => tonic::Status::internal(message),
        }
    }
}
