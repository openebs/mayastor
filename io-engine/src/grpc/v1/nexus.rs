use crate::{
    bdev::{
        nexus,
        nexus::{
            nexus_lookup_uuid_mut,
            ChildStateClient,
            FaultReason,
            NexusChild,
            NexusStatus,
        },
    },
    core::{
        lock::{ProtectedSubsystems, ResourceLockManager},
        Protocol,
        Share,
    },
    grpc::{rpc_submit, GrpcClientContext, GrpcResult},
    rebuild::{HistoryRecord, RebuildState, RebuildStats},
};
use futures::FutureExt;
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    ops::Deref,
    pin::Pin,
};
use tonic::{Request, Response, Status};

use mayastor_api::v1::nexus::*;

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

use crate::bdev::{dev::device_name, nexus::NexusPtpl, PtplFileOps};
use ::function_name::named;
use std::panic::AssertUnwindSafe;

/// RPC service for mayastor nexus operations
#[derive(Debug)]
#[allow(dead_code)]
pub struct NexusService {
    name: String,
    client_context: tokio::sync::Mutex<Option<GrpcClientContext>>,
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

    async fn serialized<T, F>(
        &self,
        ctx: GrpcClientContext,
        nexus_uuid: String,
        global_operation: bool,
        f: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
    {
        let lock_manager = ResourceLockManager::get_instance();
        let fut = AssertUnwindSafe(f).catch_unwind();

        // Schedule a Tokio task to detach it from the high-level gRPC future
        // and avoid task cancellation when the top-level gRPC future is
        // cancelled.
        match tokio::spawn(async move {
            // Grab global operation lock, if requested.
            let _global_guard = if global_operation {
                match lock_manager.lock(Some(ctx.timeout)).await {
                    Some(g) => Some(g),
                    None => return Err(Status::deadline_exceeded(
                        "Failed to acquire access to object within given timeout"
                        .to_string()
                    )),
                }
            } else {
                None
            };

            // Grab per-object lock before executing the future.
            let _resource_guard = match lock_manager
                .get_subsystem(ProtectedSubsystems::NEXUS)
                .lock_resource(nexus_uuid, Some(ctx.timeout))
                .await {
                    Some(g) => g,
                    None => return Err(Status::deadline_exceeded(
                        "Failed to acquire access to object within given timeout"
                        .to_string()
                    )),
                };
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
        })
        .await {
            Ok(r) => r,
            Err(_) => Err(Status::cancelled("gRPC call cancelled"))
        }
    }
}

impl From<NexusStatus> for NexusState {
    fn from(nexus: NexusStatus) -> Self {
        match nexus {
            NexusStatus::Faulted => NexusState::NexusFaulted,
            NexusStatus::Degraded => NexusState::NexusDegraded,
            NexusStatus::Online => NexusState::NexusOnline,
            NexusStatus::ShuttingDown => NexusState::NexusShuttingDown,
            NexusStatus::Shutdown => NexusState::NexusShutdown,
        }
    }
}

fn map_fault_reason(r: FaultReason) -> ChildStateReason {
    use ChildStateReason::*;

    match r {
        FaultReason::Unknown => None,
        FaultReason::CantOpen => CannotOpen,
        FaultReason::NoSpace => NoSpace,
        FaultReason::TimedOut => TimedOut,
        FaultReason::IoError => IoFailure,
        FaultReason::Offline => ByClient,
        FaultReason::RebuildFailed => RebuildFailed,
        FaultReason::AdminCommandFailed => AdminFailed,
        FaultReason::OfflinePermanent => ByClient,
    }
}

fn map_child_state(child: &NexusChild) -> (ChildState, ChildStateReason) {
    use ChildState::*;
    use ChildStateReason::*;

    match child.state_client() {
        ChildStateClient::Init => (Degraded, Init),
        ChildStateClient::ConfigInvalid => (Faulted, ConfigInvalid),
        ChildStateClient::Open => (Online, None),
        ChildStateClient::Closed => (Degraded, Closed),
        ChildStateClient::Faulted(r) => (
            match r {
                FaultReason::AdminCommandFailed => Faulted,
                FaultReason::IoError => Faulted,
                s if s.is_recoverable() => Degraded,
                _ => Faulted,
            },
            map_fault_reason(r),
        ),
        ChildStateClient::Faulting(r) => (Unknown, map_fault_reason(r)),
        ChildStateClient::OutOfSync => (Degraded, OutOfSync),
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
            blocks_transferred: stats.blocks_transferred,
            blocks_remaining: stats.blocks_remaining,
            progress: stats.progress,
            blocks_per_task: stats.blocks_per_task,
            block_size: stats.block_size,
            tasks_total: stats.tasks_total,
            tasks_active: stats.tasks_active,
            is_partial: stats.is_partial,
            start_time: Some(stats.start_time.into()),
        }
    }
}

impl From<RebuildState> for mayastor_api::v1::nexus::RebuildJobState {
    fn from(state: RebuildState) -> Self {
        match state {
            RebuildState::Init => RebuildJobState::Init,
            RebuildState::Running => RebuildJobState::Rebuilding,
            RebuildState::Stopped => RebuildJobState::Stopped,
            RebuildState::Paused => RebuildJobState::Paused,
            RebuildState::Failed => RebuildJobState::Failed,
            RebuildState::Completed => RebuildJobState::Completed,
        }
    }
}

impl From<&HistoryRecord> for RebuildHistoryRecord {
    fn from(record: &HistoryRecord) -> Self {
        RebuildHistoryRecord {
            child_uri: record.child_uri.clone(),
            src_uri: record.src_uri.clone(),
            state: RebuildJobState::from(record.state) as i32,
            blocks_total: record.blocks_total,
            blocks_recovered: record.blocks_recovered,
            blocks_transferred: record.blocks_transferred,
            blocks_remaining: record.blocks_remaining,
            blocks_per_task: record.blocks_per_task,
            block_size: record.block_size,
            is_partial: record.is_partial,
            start_time: Some(record.start_time.into()),
            end_time: Some(record.end_time.into()),
        }
    }
}

impl From<NvmeReservation> for nexus::NvmeReservation {
    fn from(value: NvmeReservation) -> Self {
        match value {
            NvmeReservation::Reserved => Self::Reserved,
            NvmeReservation::WriteExclusive => Self::WriteExclusive,
            NvmeReservation::ExclusiveAccess => Self::ExclusiveAccess,
            NvmeReservation::WriteExclusiveRegsOnly => {
                Self::WriteExclusiveRegsOnly
            }
            NvmeReservation::ExclusiveAccessRegsOnly => {
                Self::ExclusiveAccessRegsOnly
            }
            NvmeReservation::WriteExclusiveAllRegs => {
                Self::WriteExclusiveAllRegs
            }
            NvmeReservation::ExclusiveAccessAllRegs => {
                Self::ExclusiveAccessAllRegs
            }
        }
    }
}
struct NvmeReservationConv(Option<i32>);
impl TryFrom<NvmeReservationConv> for nexus::NvmeReservation {
    type Error = tonic::Status;
    fn try_from(value: NvmeReservationConv) -> Result<Self, Self::Error> {
        match value.0 {
            Some(v) => match NvmeReservation::from_i32(v) {
                Some(v) => Ok(v.into()),
                None => Err(tonic::Status::invalid_argument(format!(
                    "Invalid reservation type {v}"
                ))),
            },
            None => Ok(nexus::NvmeReservation::WriteExclusiveAllRegs),
        }
    }
}
struct NvmePreemptionConv(i32);
impl TryFrom<NvmePreemptionConv> for nexus::NexusNvmePreemption {
    type Error = tonic::Status;
    fn try_from(value: NvmePreemptionConv) -> Result<Self, Self::Error> {
        match NexusNvmePreemption::from_i32(value.0) {
            Some(NexusNvmePreemption::ArgKey) => {
                Ok(nexus::NexusNvmePreemption::ArgKey)
            }
            Some(NexusNvmePreemption::Holder) => {
                Ok(nexus::NexusNvmePreemption::Holder)
            }
            None => Err(tonic::Status::invalid_argument(format!(
                "Invalid reservation preempt policy {}",
                value.0
            ))),
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
    let n = nexus_lookup(uuid).map_err(|error| {
        if let Ok(uuid) = uuid::Uuid::parse_str(uuid) {
            NexusPtpl::new(uuid).destroy().ok();
        }
        error
    })?;
    n.destroy().await
}

impl<'c> NexusChild<'c> {
    async fn to_grpc_v1(&self) -> Child {
        let (s, r) = map_child_state(self);
        Child {
            uri: self.uri().to_string(),
            state: s as i32,
            state_reason: r as i32,
            rebuild_progress: self.get_rebuild_progress().await,
            device_name: self.get_device_name(),
            fault_timestamp: self.fault_timestamp().map(|d| d.into()),
        }
    }
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
            size: self.req_size(),
            state: NexusState::from(self.status()) as i32,
            device_uri: self.get_share_uri().unwrap_or_default(),
            children: {
                let mut children =
                    Vec::with_capacity(self.children_iter().count());
                for child in self.children_iter() {
                    children.push(child.to_grpc_v1().await);
                }
                children
            },
            rebuilds: self.count_rebuild_jobs() as u32,
            ana_state: ana_state as i32,
            allowed_hosts: self.allowed_hosts(),
        }
    }
}

/// Add child to nexus. Normally this would have been part of grpc method
/// implementation, however it is not allowed to use '?' in `locally` macro.
/// So we implement it as a separate function.
async fn nexus_add_child(
    args: &AddChildNexusRequest,
) -> Result<Nexus, nexus::Error> {
    let mut n = nexus_lookup(&args.uuid)?;
    if n.contains_child_uri(&args.uri) || {
        match device_name(&args.uri) {
            Ok(name) => n.contains_child_name(&name),
            _ => false,
        }
    } {
        return Err(nexus::Error::ChildAlreadyExists {
            child: args.uri.to_string(),
            name: args.uuid.to_string(),
        });
    }
    debug!("Adding child {} to nexus {} ...", args.uri, args.uuid);
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
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), true, async move {
            trace!("{:?}", args);
            let resv_type = NvmeReservationConv(args.resv_type).try_into()?;
            let preempt_policy =
                NvmePreemptionConv(args.preempt_policy).try_into()?;
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
                        resv_type,
                        preempt_policy,
                    },
                    &args.children,
                    nexus_info_key,
                )
                .await?;
                let nexus = nexus_lookup(&args.uuid)?;
                info!("Created nexus {}/{}", &args.name, &args.uuid);
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
        })
        .await
    }

    #[named]
    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<()> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), true, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                nexus_destroy(&args.uuid).await?;
                Ok(())
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn shutdown_nexus(
        &self,
        request: Request<ShutdownNexusRequest>,
    ) -> GrpcResult<ShutdownNexusResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                nexus_lookup(&args.uuid)?.shutdown().await?;

                Ok(ShutdownNexusResponse {
                    nexus: Some(nexus_lookup(&args.uuid)?.into_grpc().await),
                })
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
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
                    add_nexus(nexus_list.as_mut(), nexus).await;
                }
            } else if let Some(uuid) = args.uuid {
                let nexus = nexus_lookup(&uuid)?;
                add_nexus(nexus_list.as_mut(), &nexus).await;
            } else {
                for nexus in nexus::nexus_iter() {
                    add_nexus(nexus_list.as_mut(), nexus).await;
                }
            }

            return Ok(ListNexusResponse {
                nexus_list,
            });

            async fn add_nexus(
                nexus_list: &mut Vec<Nexus>,
                nexus: &nexus::Nexus<'_>,
            ) {
                if nexus.state.lock().deref() != &nexus::NexusState::Init {
                    nexus_list.push(nexus.into_grpc().await);
                }
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    #[named]
    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> GrpcResult<AddChildNexusResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let nexus = nexus_add_child(&args).await?;
                info!("Added child to nexus {}", args.uuid);
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
        })
        .await
    }

    #[named]
    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> GrpcResult<RemoveChildNexusResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                if nexus_lookup(&args.uuid)?.contains_child_uri(&args.uri) {
                    debug!(
                        "Removing child {} from nexus {} ...",
                        args.uri, args.uuid
                    );
                    nexus_lookup(&args.uuid)?.remove_child(&args.uri).await?;
                    info!(
                        "Removed child {} from nexus {}",
                        args.uri, args.uuid
                    );
                }
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
        })
        .await
    }

    #[named]
    async fn fault_nexus_child(
        &self,
        request: Request<FaultNexusChildRequest>,
    ) -> GrpcResult<FaultNexusChildResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                debug!("Faulting child {} on nexus {}", args.uri, args.uuid);
                nexus_lookup(&args.uuid)?
                    .fault_child(&args.uri, FaultReason::OfflinePermanent)
                    .await?;
                info!("Faulted child {} on nexus {}", args.uri, args.uuid);
                Ok(FaultNexusChildResponse {
                    nexus: Some(nexus_lookup(&args.uuid)?.into_grpc().await),
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
    async fn inject_nexus_fault(
        &self,
        request: Request<InjectNexusFaultRequest>,
    ) -> GrpcResult<()> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);

                debug!(
                    "Injecting fault to child {} of nexus {}",
                    args.uri, args.uuid
                );
                nexus_lookup(&args.uuid)?
                    .inject_add_fault(&args.uri)
                    .await?;
                info!(
                    "Injected fault to child {} of nexus {}",
                    args.uri, args.uuid
                );
                Ok(())
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn remove_injected_nexus_fault(
        &self,
        request: Request<RemoveInjectedNexusFaultRequest>,
    ) -> GrpcResult<()> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);

                debug!(
                    "Removing injected fault from child {} of nexus {}",
                    args.uri, args.uuid
                );
                nexus_lookup(&args.uuid)?
                    .inject_remove_fault(&args.uri)
                    .await?;
                info!(
                    "Removed injected fault from child {} of nexus {}",
                    args.uri, args.uuid
                );
                Ok(())
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn list_injected_nexus_faults(
        &self,
        request: Request<ListInjectedNexusFaultsRequest>,
    ) -> GrpcResult<ListInjectedNexusFaultsReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();
        trace!("{:?}", args);

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let res = nexus_lookup(&args.uuid)?
                    .list_injections()
                    .await?
                    .into_iter()
                    .map(|inj| InjectedFault {
                        device_name: inj.device_name,
                        is_active: inj.is_active,
                    })
                    .collect();

                Ok(ListInjectedNexusFaultsReply {
                    injections: res,
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
    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> GrpcResult<PublishNexusResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                debug!("Publishing nexus {} ...", args.uuid);

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
                            sp_value: args.share,
                        });
                    }
                };

                // error out if nbd or iscsi
                if !matches!(share_protocol, Protocol::Off | Protocol::Nvmf) {
                    return Err(nexus::Error::InvalidShareProtocol {
                        sp_value: args.share,
                    });
                }

                let device_uri = nexus_lookup(&args.uuid)?
                    .share_ext(share_protocol, key, args.allowed_hosts.clone())
                    .await?;

                info!(
                    "Published nexus {} under {} for {:?}",
                    args.uuid, device_uri, args.allowed_hosts
                );

                let nexus = nexus_lookup(&args.uuid)?.into_grpc().await;

                Ok(PublishNexusResponse {
                    nexus: Some(nexus),
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
    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> GrpcResult<UnpublishNexusResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
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
        })
        .await
    }

    #[named]
    async fn get_nvme_ana_state(
        &self,
        request: Request<GetNvmeAnaStateRequest>,
    ) -> GrpcResult<GetNvmeAnaStateResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let uuid = args.uuid.clone();
            debug!("Getting NVMe ANA state for nexus {} ...", uuid);

            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let ana_state =
                    nexus_lookup(&args.uuid)?.get_ana_state().await?;
                info!("Got nexus {} NVMe ANA state {:?}", uuid, ana_state);
                Ok(GetNvmeAnaStateResponse {
                    ana_state: ana_state as i32,
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
    async fn set_nvme_ana_state(
        &self,
        request: Request<SetNvmeAnaStateRequest>,
    ) -> GrpcResult<SetNvmeAnaStateResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
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
        })
        .await
    }

    #[named]
    async fn child_operation(
        &self,
        request: Request<ChildOperationRequest>,
    ) -> GrpcResult<ChildOperationResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                info!("{:?}", args);
                let mut nexus = nexus_lookup(&args.nexus_uuid)?;

                match args.action {
                    0 => {
                        nexus
                            .as_mut()
                            .fault_child(&args.uri, FaultReason::Offline)
                            .await
                    }
                    1 => nexus.as_mut().online_child(&args.uri).await,
                    2 => {
                        nexus
                            .as_mut()
                            .fault_child(&args.uri, FaultReason::IoError)
                            .await
                    }
                    3 => {
                        nexus
                            .as_mut()
                            .fault_child(
                                &args.uri,
                                FaultReason::OfflinePermanent,
                            )
                            .await
                    }
                    _ => Err(nexus::Error::InvalidKey {}),
                }?;

                Ok(nexus.into_grpc().await)
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(|n| {
                    Response::new(ChildOperationResponse {
                        nexus: Some(n),
                    })
                })
        })
        .await
    }

    #[named]
    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> GrpcResult<StartRebuildResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
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
        })
        .await
    }

    #[named]
    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> GrpcResult<StopRebuildResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
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
        })
        .await
    }

    #[named]
    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> GrpcResult<PauseRebuildResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
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
        })
        .await
    }

    #[named]
    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> GrpcResult<ResumeRebuildResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
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
        })
        .await
    }

    #[named]
    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> GrpcResult<RebuildStateResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
            info!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&args.nexus_uuid)?
                    .rebuild_state(&args.uri)
                    .map(RebuildStateResponse::from)
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn get_rebuild_stats(
        &self,
        request: Request<RebuildStatsRequest>,
    ) -> GrpcResult<RebuildStatsResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.nexus_uuid.clone(), false, async move {
            info!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&args.nexus_uuid)?
                    .rebuild_stats(&args.uri)
                    .await
                    .map(RebuildStatsResponse::from)
            })?;
            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn get_rebuild_history(
        &self,
        request: Request<RebuildHistoryRequest>,
    ) -> GrpcResult<RebuildHistoryResponse> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            trace!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let records = nexus_lookup(&args.uuid)?
                    .rebuild_history()
                    .iter()
                    .map(RebuildHistoryRecord::from)
                    .collect();
                Ok(RebuildHistoryResponse {
                    nexus: args.uuid.clone(),
                    records,
                })
            })?;
            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }
}
