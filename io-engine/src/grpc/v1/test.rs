use crate::{
    core::{
        wiper::{Error as WipeError, StreamedWiper, WipeStats, Wiper},
        VerboseError,
    },
    grpc::{
        v1::replica::{GrpcReplicaFactory, ReplicaGrpc},
        GrpcClientContext,
        GrpcResult,
        RWSerializer,
    },
    replica_backend::FindReplicaArgs,
};
use ::function_name::named;
use io_engine_api::{
    v1,
    v1::test::{
        wipe_options::WipeMethod,
        wipe_replica_request,
        wipe_replica_response,
        StreamWipeOptions,
        TestRpc,
        WipeReplicaRequest,
        WipeReplicaResponse,
    },
};
use std::convert::{TryFrom, TryInto};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::v1::pool::PoolGrpc;
#[cfg(feature = "fault-injection")]
use crate::{
    core::fault_injection::{
        add_fault_injection,
        list_fault_injections,
        remove_fault_injection,
        FaultInjectionError,
        Injection,
    },
    grpc::rpc_submit,
};

#[derive(Debug, Clone)]
pub struct TestService {
    #[allow(unused)]
    name: String,
    replica_svc: super::replica::ReplicaService,
}

impl TestService {
    pub fn new(replica_svc: super::replica::ReplicaService) -> Self {
        Self {
            name: String::from("TestSvc"),
            replica_svc,
        }
    }
}

#[tonic::async_trait]
impl TestRpc for TestService {
    type WipeReplicaStream =
        ReceiverStream<Result<WipeReplicaResponse, Status>>;

    /// Get all the features supported by the test service.
    async fn get_features(
        &self,
        _request: Request<()>,
    ) -> GrpcResult<v1::test::TestFeatures> {
        GrpcResult::Ok(tonic::Response::new(v1::test::TestFeatures {
            wipe_methods: vec![
                v1::test::wipe_options::WipeMethod::None as i32,
                v1::test::wipe_options::WipeMethod::WriteZeroes as i32,
                v1::test::wipe_options::WipeMethod::Checksum as i32,
            ],
            cksum_algs: vec![
                v1::test::wipe_options::CheckSumAlgorithm::Crc32c as i32,
            ],
        }))
    }

    #[named]
    async fn wipe_replica(
        &self,
        request: Request<WipeReplicaRequest>,
    ) -> Result<Response<Self::WipeReplicaStream>, Status> {
        // This is to avoid having to use a trampoline to send channels between
        // different reactors.
        let max_chunks = 1024;
        let (tx, rx) = tokio::sync::mpsc::channel(max_chunks);

        let replica_svc = self.replica_svc.clone();
        let tx_cln = tx.clone();
        let options = crate::core::wiper::StreamWipeOptions::try_from(
            &request.get_ref().wipe_options,
        )?;
        let uuid = request.get_ref().uuid.clone();

        crate::core::spawn(async move {
            let result = replica_svc
                .shared(
                    GrpcClientContext::new(&request, function_name!()),
                    async move {
                        let args = request.into_inner();
                        info!("{:?}", args);
                        crate::spdk_submit!(async move {
                            let pool = match args.pool {
                                Some(pool) => Some(
                                    GrpcReplicaFactory::pool_finder(pool)
                                        .await?,
                                ),
                                None => None,
                            };
                            let args = FindReplicaArgs::new(&args.uuid);
                            let repl = GrpcReplicaFactory::finder(
                                // until API is modified to allow snapshots
                                &args.allow_snapshots(),
                            )
                            .await?;
                            validate_pool(&repl, pool).await?;

                            let wiper = repl.wiper(options.wipe_method)?;

                            let proto_stream = WiperStream(tx_cln);
                            let wiper = StreamedWiper::new(
                                wiper,
                                options.chunk_size,
                                max_chunks,
                                proto_stream,
                            )?;
                            let final_stats = wiper.wipe().await?;
                            final_stats.log();
                            Ok(())
                        })
                    },
                )
                .await;
            if tx.is_closed() {
                tracing::error!("Wipe of {uuid} aborted: client disconnected");
            } else if let Err(error) = result {
                tracing::error!("Wipe of {uuid} failed: {error}");
                tx.send(Err(error)).await.ok();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn add_fault_injection(
        &self,
        request: Request<v1::test::AddFaultInjectionRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();
        trace!("{:?}", args);

        #[cfg(feature = "fault-injection")]
        {
            let rx = rpc_submit::<_, _, FaultInjectionError>(async move {
                let uri = args.uri.clone();
                let inj = Injection::from_uri(&uri)?;
                add_fault_injection(inj)?;
                Ok(())
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        }

        #[cfg(not(feature = "fault-injection"))]
        GrpcResult::Err(tonic::Status::unimplemented(
            "Fault injection feature is disabled",
        ))
    }

    async fn remove_fault_injection(
        &self,
        request: Request<v1::test::RemoveFaultInjectionRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();
        trace!("{:?}", args);

        #[cfg(feature = "fault-injection")]
        {
            let rx = rpc_submit::<_, _, FaultInjectionError>(async move {
                let uri = args.uri.clone();

                // Validate injection URI by trying to parse it.
                Injection::from_uri(&uri)?;

                remove_fault_injection(&uri)?;

                Ok(())
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        }

        #[cfg(not(feature = "fault-injection"))]
        GrpcResult::Err(tonic::Status::unimplemented(
            "Fault injection feature is disabled",
        ))
    }

    async fn list_fault_injections(
        &self,
        request: Request<v1::test::ListFaultInjectionsRequest>,
    ) -> GrpcResult<v1::test::ListFaultInjectionsReply> {
        let args = request.into_inner();
        trace!("{:?}", args);

        #[cfg(feature = "fault-injection")]
        {
            let rx = rpc_submit::<_, _, FaultInjectionError>(async move {
                let injections = list_fault_injections()
                    .into_iter()
                    .map(v1::test::FaultInjection::from)
                    .collect();

                Ok(v1::test::ListFaultInjectionsReply {
                    injections,
                })
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        }

        #[cfg(not(feature = "fault-injection"))]
        GrpcResult::Err(tonic::Status::unimplemented(
            "Fault injection feature is disabled",
        ))
    }
}

impl TryFrom<&Option<StreamWipeOptions>>
    for crate::core::wiper::StreamWipeOptions
{
    type Error = tonic::Status;

    fn try_from(
        value: &Option<StreamWipeOptions>,
    ) -> Result<Self, Self::Error> {
        let Some(wipe) = value else {
            return Err(tonic::Status::invalid_argument(
                "Missing StreamWipeOptions",
            ));
        };
        let Some(options) = &wipe.options else {
            return Err(tonic::Status::invalid_argument("Missing WipeOptions"));
        };

        Ok(crate::core::wiper::StreamWipeOptions {
            chunk_size: wipe.chunk_size,
            wipe_method: {
                let method = WipeMethod::try_from(options.wipe_method)
                    .map_err(|_| {
                        tonic::Status::invalid_argument("Invalid Wipe Method")
                    })?;
                Wiper::supported(match method {
                    WipeMethod::None => crate::core::wiper::WipeMethod::None,
                    WipeMethod::WriteZeroes => {
                        crate::core::wiper::WipeMethod::WriteZeroes
                    }
                    WipeMethod::Unmap => crate::core::wiper::WipeMethod::Unmap,
                    WipeMethod::WritePattern => {
                        crate::core::wiper::WipeMethod::WritePattern(
                            options.write_pattern.unwrap_or(0xdeadbeef),
                        )
                    }
                    WipeMethod::Checksum => {
                        crate::core::wiper::WipeMethod::CkSum(
                            crate::core::wiper::CkSumMethod::default(),
                        )
                    }
                })
                .map_err(|error| {
                    tonic::Status::invalid_argument(error.to_string())
                })?
            },
        })
    }
}

impl From<&WipeStats> for WipeReplicaResponse {
    fn from(value: &WipeStats) -> Self {
        Self {
            uuid: value.uuid.to_string(),
            total_bytes: value.total_bytes,
            chunk_size: value.chunk_size_bytes,
            last_chunk_size: value
                .extra_chunk_size_bytes
                .unwrap_or(value.chunk_size_bytes),
            total_chunks: value.total_chunks,
            wiped_bytes: value.wiped_bytes,
            wiped_chunks: value.wiped_chunks,
            remaining_bytes: value.total_bytes - value.wiped_bytes,
            since: value.since.and_then(|d| TryInto::try_into(d).ok()),
            checksum: value
                .cksum_crc32c
                .map(wipe_replica_response::Checksum::Crc32),
        }
    }
}

impl From<WipeError> for tonic::Status {
    fn from(value: WipeError) -> Self {
        match value {
            WipeError::TooManyChunks {
                ..
            } => Self::invalid_argument(value.to_string()),
            WipeError::ChunkTooLarge {
                ..
            } => Self::invalid_argument(value.to_string()),
            WipeError::ZeroBdev {
                ..
            } => Self::invalid_argument(value.to_string()),
            WipeError::ChunkBlockSizeInvalid {
                ..
            } => Self::invalid_argument(value.to_string()),
            WipeError::WipeIoFailed {
                ..
            } => Self::data_loss(value.verbose()),
            WipeError::MethodUnimplemented {
                ..
            } => Self::invalid_argument(value.to_string()),
            WipeError::ChunkNotifyFailed {
                ..
            } => Self::internal(value.to_string()),
            WipeError::WipeAborted {
                ..
            } => Self::aborted(value.to_string()),
        }
    }
}

impl From<wipe_replica_request::Pool> for crate::pool_backend::FindPoolArgs {
    fn from(value: wipe_replica_request::Pool) -> Self {
        match value {
            wipe_replica_request::Pool::PoolName(name) => {
                Self::name_uuid(name, &None)
            }
            wipe_replica_request::Pool::PoolUuid(uuid) => Self::uuid(uuid),
        }
    }
}

/// Validate that the replica belongs to the specified pool.
async fn validate_pool(
    repl: &ReplicaGrpc,
    pool: Option<PoolGrpc>,
) -> Result<(), Status> {
    let Some(pool) = pool else {
        return Ok(());
    };

    repl.verify_pool(&pool)
}

struct WiperStream(
    tokio::sync::mpsc::Sender<Result<WipeReplicaResponse, tonic::Status>>,
);

impl crate::core::wiper::NotifyStream for WiperStream {
    fn notify(&self, stats: &WipeStats) -> Result<(), String> {
        let response = WipeReplicaResponse::from(stats);
        // `send()` may get stuck if we send/receive from different reactors so
        // for now let's simply use try_send, which will fail if the max
        // buffers are reached.
        self.0.try_send(Ok(response)).map_err(|e| e.to_string())
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

#[cfg(feature = "fault-injection")]
impl From<FaultInjectionError> for tonic::Status {
    fn from(e: FaultInjectionError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}

#[cfg(feature = "fault-injection")]
impl From<Injection> for v1::test::FaultInjection {
    fn from(src: Injection) -> Self {
        let is_active = src.is_active();
        Self {
            uri: src.uri(),
            device_name: src.device_name,
            is_active,
        }
    }
}
