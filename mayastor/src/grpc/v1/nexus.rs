use crate::{
    bdev::{
        nexus,
        nexus::{nexus_lookup_uuid_mut, NexusChild, NexusStatus, Reason},
    },
    core::{Protocol, Share},
    grpc::{rpc_submit, GrpcClientContext, GrpcResult, Serializer},
    rebuild::{RebuildJob, RebuildState, RebuildStats},
};
use futures::FutureExt;
use std::{convert::TryFrom, fmt::Debug, ops::Deref, pin::Pin};
use tonic::{Request, Response, Status};

use rpc::mayastor::v1::nexus::*;

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

use ::function_name::named;
use std::panic::AssertUnwindSafe;

/// RPC service for mayastor nexus operations
#[derive(Debug)]
#[allow(dead_code)]
pub struct NexusService {
    name: String,
    client_context: tokio::sync::Mutex<Option<GrpcClientContext>>,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for NexusService
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

impl Default for NexusService {
    fn default() -> Self {
        Self::new()
    }
}

impl NexusService {
    pub fn new() -> Self {
        Self {
            name: String::from("NexusService"),
            client_context: tokio::sync::Mutex::new(None),
        }
    }
}

impl From<NexusStatus> for NexusState {
    fn from(nexus: NexusStatus) -> Self {
        match nexus {
            NexusStatus::Faulted => NexusState::NexusFaulted,
            NexusStatus::Degraded => NexusState::NexusDegraded,
            NexusStatus::Online => NexusState::NexusOnline,
        }
    }
}

/// Map the internal child states into rpc child states (i.e. the states that
/// the control plane sees)
impl From<nexus::ChildState> for ChildState {
    fn from(child: nexus::ChildState) -> Self {
        match child {
            nexus::ChildState::Init => ChildState::ChildDegraded,
            nexus::ChildState::ConfigInvalid => ChildState::ChildFaulted,
            nexus::ChildState::Open => ChildState::ChildOnline,
            nexus::ChildState::Destroying => ChildState::ChildDegraded,
            nexus::ChildState::Closed => ChildState::ChildDegraded,
            nexus::ChildState::Faulted(reason) => match reason {
                Reason::OutOfSync => ChildState::ChildDegraded,
                _ => ChildState::ChildFaulted,
            },
        }
    }
}

impl<'c> From<&NexusChild<'c>> for Child {
    fn from(ch: &NexusChild) -> Self {
        Child {
            uri: ch.get_name().to_string(),
            state: ChildState::from(ch.state()) as i32,
            rebuild_progress: ch.get_rebuild_progress(),
        }
    }
}

impl From<RebuildState> for RebuildStateResponse {
    fn from(rs: RebuildState) -> Self {
        RebuildStateResponse {
            state: rs.to_string(),
        }
    }
}

impl From<RebuildStats> for RebuildStatsResponse {
    fn from(stats: RebuildStats) -> Self {
        RebuildStatsResponse {
            blocks_total: stats.blocks_total,
            blocks_recovered: stats.blocks_recovered,
            progress: stats.progress,
            segment_size_blks: stats.segment_size_blks,
            block_size: stats.block_size,
            tasks_total: stats.tasks_total,
            tasks_active: stats.tasks_active,
        }
    }
}

/// Look up a nexus by uuid
pub fn nexus_lookup<'n>(
    uuid: &str,
) -> Result<Pin<&'n mut nexus::Nexus<'n>>, nexus::Error> {
    if let Some(nexus) = nexus_lookup_uuid_mut(uuid) {
        Ok(nexus)
    } else {
        Err(nexus::Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

/// Destruction of the nexus. Returns NotFound error for invalid uuid.
pub async fn nexus_destroy(uuid: &str) -> Result<(), nexus::Error> {
    let n = nexus_lookup(uuid)?;
    n.destroy().await
}

impl<'n> nexus::Nexus<'n> {
    /// Convert nexus object to grpc representation.
    ///
    /// We cannot use From trait because it is not value to value conversion.
    /// All we have is a reference to nexus.

    pub async fn into_grpc(&self) -> Nexus {
        let mut ana_state = NvmeAnaState::NvmeAnaInvalidState;

        // Get ANA state only for published nexuses.
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Ok(state) = self.get_ana_state().await {
                ana_state = NvmeAnaState::from_i32(state as i32)
                    .unwrap_or(NvmeAnaState::NvmeAnaInvalidState);
            }
        }

        Nexus {
            name: self.name.clone(),
            uuid: self.uuid().to_string(),
            size: self.req_size,
            state: NexusState::from(self.status()) as i32,
            device_uri: self.get_share_uri().unwrap_or_default(),
            children: self
                .children
                .iter()
                .map(|ch| ch.into())
                .collect::<Vec<_>>(),
            rebuilds: RebuildJob::count() as u32,
            ana_state: ana_state as i32,
        }
    }
}

/// Add child to nexus. Normally this would have been part of grpc method
/// implementation, however it is not allowed to use '?' in `locally` macro.
/// So we implement it as a separate function.
async fn nexus_add_child(
    args: AddChildNexusRequest,
) -> Result<Nexus, nexus::Error> {
    let mut n = nexus_lookup(&args.uuid)?;
    // TODO: do not add child if it already exists (idempotency)
    // For that we need api to check existence of child by name (not uri that
    // contain parameters that may change).
    n.as_mut().add_child(&args.uri, args.norebuild).await?;
    Ok(n.into_grpc().await)
}

#[tonic::async_trait]
impl NexusRpc for NexusService {
    #[named]
    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> GrpcResult<CreateNexusResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);

                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    // check for nexus exists, uuid & name
                    if let Some(_n) = nexus::nexus_lookup(&args.name) {
                        return Err(nexus::Error::NameExists {
                            name: args.name.clone(),
                        });
                    }
                    if let Ok(_n) = nexus_lookup(&args.uuid) {
                        return Err(nexus::Error::UuidExists {
                            uuid: args.uuid.clone(),
                            nexus: args.name.clone(),
                        });
                    }

                    // If the control plane has supplied a key, use it to store
                    // the NexusInfo.
                    let nexus_info_key = if args.nexus_info_key.is_empty() {
                        None
                    } else {
                        Some(args.nexus_info_key.to_string())
                    };

                    nexus::nexus_create_v2(
                        &args.name,
                        args.size,
                        &args.uuid,
                        nexus::NexusNvmeParams {
                            min_cntlid: args.min_cntl_id as u16,
                            max_cntlid: args.max_cntl_id as u16,
                            resv_key: args.resv_key,
                            preempt_key: match args.preempt_key {
                                0 => None,
                                k => std::num::NonZeroU64::new(k),
                            },
                        },
                        &args.children,
                        nexus_info_key,
                    )
                    .await?;
                    let nexus = nexus_lookup(&args.uuid)?;
                    info!("Created nexus {}", &args.name);
                    Ok(nexus.into_grpc().await)
                })?;
                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(|nexus| {
                        Response::new(CreateNexusResponse {
                            nexus: Some(nexus),
                        })
                    })
            },
        )
        .await
    }

    #[named]
    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<()> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    let args = request.into_inner();
                    trace!("{:?}", args);
                    nexus_destroy(&args.uuid).await?;
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

    async fn list_nexus(
        &self,
        request: Request<ListNexusOptions>,
    ) -> GrpcResult<ListNexusResponse> {
        let args = request.into_inner();
        trace!("{:?}", args);

        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            let mut nexus_list: Vec<Nexus> = Vec::new();
            if let Some(name) = args.name {
                if let Some(nexus) = nexus::nexus_lookup(&name) {
                    nexus_list.push(nexus.into_grpc().await);
                }
            } else {
                for n in nexus::nexus_iter() {
                    if n.state.lock().deref() != &nexus::NexusState::Init {
                        nexus_list.push(n.into_grpc().await);
                    }
                }
            }

            Ok(ListNexusResponse {
                nexus_list,
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> GrpcResult<AddChildNexusResponse> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Adding child {} to nexus {} ...", args.uri, uuid);
            let nexus = nexus_add_child(args).await?;
            info!("Added child to nexus {}", uuid);
            Ok(nexus)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(|nexus| {
                Response::new(AddChildNexusResponse {
                    nexus: Some(nexus),
                })
            })
    }

    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> GrpcResult<RemoveChildNexusResponse> {
        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Removing child {} from nexus {} ...", args.uri, uuid);
            nexus_lookup(&args.uuid)?.remove_child(&args.uri).await?;
            info!("Removed child from nexus {}", uuid);
            Ok(nexus_lookup(&args.uuid)?.into_grpc().await)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(|nexus| {
                Response::new(RemoveChildNexusResponse {
                    nexus: Some(nexus),
                })
            })
    }

    async fn fault_nexus_child(
        &self,
        request: Request<FaultNexusChildRequest>,
    ) -> GrpcResult<()> {
        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            let uri = args.uri.clone();
            debug!("Faulting child {} on nexus {}", uri, uuid);
            nexus_lookup(&args.uuid)?
                .fault_child(&args.uri, nexus::Reason::Rpc)
                .await?;
            info!("Faulted child {} on nexus {}", uri, uuid);
            Ok(())
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> GrpcResult<PublishNexusResponse> {
        let rx = rpc_submit::<_, _, nexus::Error>(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Publishing nexus {} ...", uuid);

            if !args.key.is_empty() && args.key.len() != 16 {
                return Err(nexus::Error::InvalidKey {});
            }

            let key: Option<String> = if args.key.is_empty() {
                None
            } else {
                Some(args.key.clone())
            };

            let share_protocol = match Protocol::try_from(args.share) {
                Ok(protocol) => protocol,
                Err(_) => {
                    return Err(nexus::Error::InvalidShareProtocol {
                        sp_value: args.share as i32,
                    });
                }
            };

            // error out if nbd or iscsi
            if !matches!(share_protocol, Protocol::Off | Protocol::Nvmf) {
                return Err(nexus::Error::InvalidShareProtocol {
                    sp_value: args.share as i32,
                });
            }

            let device_uri =
                nexus_lookup(&args.uuid)?.share(share_protocol, key).await?;

            info!("Published nexus {} under {}", uuid, device_uri);

            let nexus = nexus_lookup(&args.uuid)?.into_grpc().await;

            Ok(PublishNexusResponse {
                nexus: Some(nexus),
            })
        })?;
        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> GrpcResult<UnpublishNexusResponse> {
        let rx = rpc_submit::<_, _, nexus::Error>(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Unpublishing nexus {} ...", uuid);
            nexus_lookup(&args.uuid)?.unshare_nexus().await?;
            info!("Unpublished nexus {}", uuid);
            Ok(nexus_lookup(&args.uuid)?.into_grpc().await)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(|nexus| {
                Response::new(UnpublishNexusResponse {
                    nexus: Some(nexus),
                })
            })
    }

    async fn get_nvme_ana_state(
        &self,
        request: Request<GetNvmeAnaStateRequest>,
    ) -> GrpcResult<GetNvmeAnaStateResponse> {
        let args = request.into_inner();
        let uuid = args.uuid.clone();
        debug!("Getting NVMe ANA state for nexus {} ...", uuid);

        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            let ana_state = nexus_lookup(&args.uuid)?.get_ana_state().await?;
            info!("Got nexus {} NVMe ANA state {:?}", uuid, ana_state);
            Ok(GetNvmeAnaStateResponse {
                ana_state: ana_state as i32,
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn set_nvme_ana_state(
        &self,
        request: Request<SetNvmeAnaStateRequest>,
    ) -> GrpcResult<SetNvmeAnaStateResponse> {
        let args = request.into_inner();
        let uuid = args.uuid.clone();
        debug!("Setting NVMe ANA state for nexus {} ...", uuid);

        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            let ana_state = nexus::NvmeAnaState::from_i32(args.ana_state)?;

            let ana_state =
                nexus_lookup(&args.uuid)?.set_ana_state(ana_state).await?;
            info!("Set nexus {} NVMe ANA state {:?}", uuid, ana_state);
            Ok(nexus_lookup(&args.uuid)?.into_grpc().await)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(|nexus| {
                Response::new(SetNvmeAnaStateResponse {
                    nexus: Some(nexus),
                })
            })
    }

    #[named]
    async fn child_operation(
        &self,
        request: Request<ChildOperationRequest>,
    ) -> GrpcResult<ChildOperationResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    let args = request.into_inner();
                    info!("{:?}", args);

                    let online = match args.action {
                        1 => Ok(true),
                        0 => Ok(false),
                        _ => Err(nexus::Error::InvalidKey {}),
                    }?;

                    let nexus = nexus_lookup(&args.nexus_uuid)?;
                    if online {
                        nexus.online_child(&args.uri).await?;
                    } else {
                        nexus.offline_child(&args.uri).await?;
                    }

                    Ok(nexus_lookup(&args.nexus_uuid)?.into_grpc().await)
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(|n| {
                        Response::new(ChildOperationResponse {
                            nexus: Some(n),
                        })
                    })
            },
        )
        .await
    }

    #[named]
    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> GrpcResult<StartRebuildResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    nexus_lookup(&args.nexus_uuid)?
                        .start_rebuild(&args.uri)
                        .await
                        // todo
                        .map(|_| {})?;
                    Ok(nexus_lookup(&args.nexus_uuid)?.into_grpc().await)
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(|n| {
                        Response::new(StartRebuildResponse {
                            nexus: Some(n),
                        })
                    })
            },
        )
        .await
    }

    #[named]
    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> GrpcResult<StopRebuildResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    nexus_lookup(&args.nexus_uuid)?
                        .stop_rebuild(&args.uri)
                        .await?;

                    Ok(nexus_lookup(&args.nexus_uuid)?.into_grpc().await)
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(|n| {
                        Response::new(StopRebuildResponse {
                            nexus: Some(n),
                        })
                    })
            },
        )
        .await
    }

    #[named]
    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> GrpcResult<PauseRebuildResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    nexus_lookup(&args.nexus_uuid)?
                        .pause_rebuild(&args.uri)
                        .await?;

                    Ok(nexus_lookup(&args.nexus_uuid)?.into_grpc().await)
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(|n| {
                        Response::new(PauseRebuildResponse {
                            nexus: Some(n),
                        })
                    })
            },
        )
        .await
    }

    #[named]
    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> GrpcResult<ResumeRebuildResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    nexus_lookup(&args.nexus_uuid)?
                        .resume_rebuild(&args.uri)
                        .await?;
                    Ok(nexus_lookup(&args.nexus_uuid)?.into_grpc().await)
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(|n| {
                        Response::new(ResumeRebuildResponse {
                            nexus: Some(n),
                        })
                    })
            },
        )
        .await
    }

    #[named]
    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> GrpcResult<RebuildStateResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    nexus_lookup(&args.nexus_uuid)?
                        .get_rebuild_state(&args.uri)
                        .await
                        .map(RebuildStateResponse::from)
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
    async fn get_rebuild_stats(
        &self,
        request: Request<RebuildStatsRequest>,
    ) -> GrpcResult<RebuildStatsResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    nexus_lookup(&args.nexus_uuid)?
                        .get_rebuild_stats(&args.uri)
                        .await
                        .map(RebuildStatsResponse::from)
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
