use crate::{
    bdev_api::BdevError,
    core::{
        wiper::{Error as WipeError, StreamedWiper, WipeStats, Wiper},
        Bdev,
        VerboseError,
    },
    grpc::{rpc_submit, GrpcClientContext, GrpcResult, Serializer},
    lvs::{Error as LvsError, Lvol, Lvs, LvsLvol},
};
use ::function_name::named;
use mayastor_api::{
    v1,
    v1::test::{
        wipe_options::WipeMethod,
        wipe_replica_request,
        StreamWipeOptions,
        TestRpc,
        WipeReplicaRequest,
        WipeReplicaResponse,
    },
};
use nix::errno::Errno;
use std::convert::{TryFrom, TryInto};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[cfg(feature = "fault-injection")]
use crate::core::fault_injection::{
    add_fault_injection,
    list_fault_injections,
    remove_fault_injection,
    FaultInjectionError,
    Injection,
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
                .locked(
                    GrpcClientContext::new(&request, function_name!()),
                    async move {
                        let args = request.into_inner();
                        info!("{:?}", args);
                        let rx = rpc_submit(async move {
                            let lvol = Bdev::lookup_by_uuid_str(&args.uuid)
                                .ok_or(LvsError::InvalidBdev {
                                    source: BdevError::BdevNotFound {
                                        name: args.uuid.clone(),
                                    },
                                    name: args.uuid,
                                })
                                .and_then(Lvol::try_from)?;
                            validate_pool(&lvol, args.pool)?;

                            let wiper = lvol.wiper(options.wipe_method)?;

                            let proto_stream = WiperStream(tx_cln);
                            let wiper = StreamedWiper::new(
                                wiper,
                                options.chunk_size,
                                max_chunks,
                                proto_stream,
                            )?;
                            let final_stats = wiper.wipe().await?;
                            final_stats.log();
                            Result::<(), LvsError>::Ok(())
                        })?;
                        rx.await
                            .map_err(|_| Status::cancelled("cancelled"))?
                            .map_err(Status::from)
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
                let method = WipeMethod::from_i32(options.wipe_method).ok_or(
                    tonic::Status::invalid_argument("Invalid Wipe Method"),
                )?;
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

/// Validate that the specified pool contains the specified lvol.
fn validate_pool(
    lvol: &Lvol,
    pool: Option<wipe_replica_request::Pool>,
) -> Result<(), LvsError> {
    let Some(pool) = pool else {
        return Ok(());
    };

    let lvs = lookup_pool(pool)?;
    if lvol.lvs().uuid() == lvs.uuid() && lvol.lvs().name() == lvs.name() {
        return Ok(());
    }

    let msg = format!("Specified {lvs:?} does match the target {lvol:?}!");
    tracing::error!("{msg}");
    Err(LvsError::Invalid {
        source: Errno::EINVAL,
        msg,
    })
}

fn lookup_pool(pool: wipe_replica_request::Pool) -> Result<Lvs, LvsError> {
    match pool {
        wipe_replica_request::Pool::PoolUuid(uuid) => {
            Lvs::lookup_by_uuid(&uuid).ok_or(LvsError::PoolNotFound {
                source: Errno::ENOMEDIUM,
                msg: format!("Pool uuid={uuid} is not loaded"),
            })
        }
        wipe_replica_request::Pool::PoolName(name) => {
            Lvs::lookup(&name).ok_or(LvsError::PoolNotFound {
                source: Errno::ENOMEDIUM,
                msg: format!("Pool name={name} is not loaded"),
            })
        }
    }
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
        match e {
            e => Status::invalid_argument(e.to_string()),
        }
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
