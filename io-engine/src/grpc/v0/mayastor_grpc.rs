//! Mayastor grpc methods implementation.
//!
//! The Mayastor gRPC methods serve as a higher abstraction for provisioning
//! replicas and targets to be used with CSI.
//
//! We want to keep the code here to a minimal, for example grpc/pool.rs
//! contains all the conversions and mappings etc to whatever interface from a
//! grpc perspective we provide. Also, by doing his, we can test the methods
//! without the need for setting up a grpc client.

use crate::{
    bdev::{
        nexus,
        nexus::{
            Error as NexusError,
            FaultReason,
            NexusReplicaSnapshotDescriptor,
        },
        NvmeControllerState as ControllerState,
        PtplFileOps,
    },
    bdev_api::BdevError,
    core::{
        lock::{ProtectedSubsystems, ResourceLockManager},
        logical_volume::LogicalVolume,
        BlockDeviceIoStats,
        CoreError,
        MayastorFeatures,
        Protocol,
        Share,
        ShareProps,
        SnapshotParams,
        UntypedBdev,
    },
    grpc::{
        controller_grpc::{
            controller_stats,
            list_controllers,
            NvmeControllerInfo,
        },
        rpc_submit,
        v0::nexus_grpc::{
            nexus_add_child,
            nexus_destroy,
            nexus_lookup,
            uuid_to_name,
        },
        GrpcClientContext,
        GrpcResult,
        Serializer,
    },
    host::{blk_device, resource},
    lvs::{lvs_lvol::LvsLvol, Error as LvsError, Lvol, LvolSpaceUsage, Lvs},
    pool_backend::PoolArgs,
    rebuild::{RebuildState, RebuildStats},
    subsys::PoolConfig,
};

use chrono::Utc;
use futures::FutureExt;
use mayastor_api::v0::*;
use nix::errno::Errno;
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    ops::Deref,
    time::Duration,
};
use tonic::{Request, Response, Status};
/// TODO
#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

use crate::core::{UpdateProps, VerboseError};
use ::function_name::named;
use std::{panic::AssertUnwindSafe, pin::Pin};
use uuid::Uuid;
use version_info::raw_version_string;

#[derive(Debug)]
#[allow(dead_code)]
pub struct MayastorSvc {
    name: String,
    interval: Duration,
    rw_lock: tokio::sync::RwLock<Option<GrpcClientContext>>,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for MayastorSvc
where
    T: Send + 'static,
    F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
{
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        let mut guard = self.rw_lock.write().await;

        // Store context as a marker of to detect abnormal termination of the
        // request. Even though AssertUnwindSafe() allows us to
        // intercept asserts in underlying method strategies, such a
        // situation can still happen when the high-level future that
        // represents gRPC call at the highest level (i.e. the one created
        // by gRPC server) gets cancelled (due to timeout or somehow else).
        // This can't be properly intercepted by 'locked' function itself in the
        // first place, so the state needs to be cleaned up properly
        // upon subsequent gRPC calls.
        if let Some(c) = guard.replace(ctx) {
            warn!("{}: gRPC method timed out, args: {}", c.id, c.args);
        }

        let fut = AssertUnwindSafe(f).catch_unwind();
        let r = fut.await;

        // Request completed, remove the marker.
        let ctx = guard.take().expect("gRPC context disappeared");

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

impl MayastorSvc {
    pub fn new(interval: Duration) -> Self {
        Self {
            name: String::from("CSISvc"),
            interval,
            rw_lock: tokio::sync::RwLock::new(None),
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

impl TryFrom<CreatePoolRequest> for PoolArgs {
    type Error = LvsError;
    fn try_from(args: CreatePoolRequest) -> Result<Self, Self::Error> {
        match args.disks.len() {
            0 => Err(LvsError::Invalid {
                source: Errno::EINVAL,
                msg: "invalid argument, missing devices".to_string(),
            }),
            _ => Ok(Self {
                name: args.name,
                disks: args.disks,
                uuid: None,
            }),
        }
    }
}

impl From<LvsError> for tonic::Status {
    fn from(e: LvsError) -> Self {
        match e {
            LvsError::Import {
                ..
            } => Status::invalid_argument(e.to_string()),
            LvsError::RepCreate {
                source, ..
            } => {
                if source == Errno::ENOSPC {
                    Status::resource_exhausted(e.to_string())
                } else {
                    Status::invalid_argument(e.to_string())
                }
            }
            LvsError::RepDestroy {
                source, ..
            } => match source {
                Errno::ENOENT => {
                    let mut status = Status::not_found(e.to_string());
                    status.metadata_mut().insert(
                        "gtm-602",
                        tonic::metadata::MetadataValue::from(0),
                    );
                    status
                }
                Errno::ENOMEDIUM => Status::failed_precondition(e.to_string()),
                Errno::EMEDIUMTYPE => Status::aborted(e.to_string()),
                _ => Status::internal(e.to_string()),
            },
            LvsError::RepExists {
                ..
            } => Status::already_exists(e.to_string()),
            LvsError::ReplicaShareProtocol {
                ..
            } => Status::invalid_argument(e.to_string()),
            LvsError::Destroy {
                source, ..
            } => source.into(),
            LvsError::Invalid {
                source, ..
            } => match source {
                Errno::EINVAL => Status::invalid_argument(e.to_string()),
                Errno::ENOMEDIUM => Status::failed_precondition(e.to_string()),
                Errno::ENOENT => Status::not_found(e.to_string()),
                Errno::EEXIST => Status::already_exists(e.to_string()),
                _ => Status::invalid_argument(e.to_string()),
            },
            LvsError::PoolNotFound {
                ..
            } => Status::not_found(e.to_string()),
            LvsError::PoolCreate {
                source, ..
            } => {
                if source == Errno::EEXIST {
                    Status::already_exists(e.to_string())
                } else if source == Errno::EINVAL {
                    Status::invalid_argument(e.to_string())
                } else {
                    Status::internal(e.to_string())
                }
            }
            LvsError::InvalidBdev {
                source, ..
            } => source.into(),
            _ => Status::internal(e.verbose()),
        }
    }
}

impl From<Protocol> for i32 {
    fn from(p: Protocol) -> Self {
        match p {
            Protocol::Off => 0,
            Protocol::Nvmf => 1,
        }
    }
}

impl From<Lvs> for Pool {
    fn from(l: Lvs) -> Self {
        Self {
            name: l.name().into(),
            disks: vec![l.base_bdev().bdev_uri().unwrap_or_else(|| "".into())],
            state: PoolState::PoolOnline.into(),
            capacity: l.capacity(),
            used: l.used(),
        }
    }
}

impl From<BlockDeviceIoStats> for Stats {
    fn from(b: BlockDeviceIoStats) -> Self {
        Self {
            num_read_ops: b.num_read_ops,
            num_write_ops: b.num_write_ops,
            bytes_read: b.bytes_read,
            bytes_written: b.bytes_written,
        }
    }
}

impl From<Lvol> for Replica {
    fn from(l: Lvol) -> Self {
        Self {
            uuid: l.name(),
            pool: l.pool_name(),
            thin: l.is_thin(),
            size: l.size(),
            share: l.shared().unwrap().into(),
            uri: l.share_uri().unwrap(),
            allowed_hosts: l.allowed_hosts(),
        }
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
        }
    }
}

impl From<Lvol> for ReplicaV2 {
    fn from(l: Lvol) -> Self {
        let usage = l.usage();
        Self {
            name: l.name(),
            uuid: l.uuid(),
            pool: l.pool_name(),
            thin: l.is_thin(),
            size: usage.capacity_bytes,
            share: l.shared().unwrap().into(),
            uri: l.share_uri().unwrap(),
            usage: Some(usage.into()),
            allowed_hosts: l.allowed_hosts(),
        }
    }
}

impl From<RebuildState> for RebuildStateReply {
    fn from(rs: RebuildState) -> Self {
        RebuildStateReply {
            state: rs.to_string(),
        }
    }
}

impl From<RebuildStats> for RebuildStatsReply {
    fn from(stats: RebuildStats) -> Self {
        RebuildStatsReply {
            blocks_total: stats.blocks_total,
            blocks_recovered: stats.blocks_recovered,
            progress: stats.progress,
            segment_size_blks: stats.blocks_per_task,
            block_size: stats.block_size,
            tasks_total: stats.tasks_total,
            tasks_active: stats.tasks_active,
        }
    }
}

impl From<MayastorFeatures> for mayastor_api::v0::MayastorFeatures {
    fn from(f: MayastorFeatures) -> Self {
        Self {
            asymmetric_namespace_access: f.asymmetric_namespace_access,
        }
    }
}

impl From<blk_device::BlockDevice> for BlockDevice {
    fn from(b: blk_device::BlockDevice) -> Self {
        Self {
            devname: b.devname,
            devtype: b.devtype,
            devmajor: b.devmaj,
            devminor: b.devmin,
            model: b.model,
            devpath: b.devpath,
            devlinks: b.devlinks,
            size: b.size,
            partition: b.partition.map(block_device::Partition::from),
            filesystem: b.filesystem.map(block_device::Filesystem::from),
            available: b.available,
        }
    }
}

impl From<blk_device::FileSystem> for block_device::Filesystem {
    fn from(fs: blk_device::FileSystem) -> Self {
        let mountpoint = fs.mountpoints.get(0).cloned().unwrap_or_default();
        Self {
            fstype: fs.fstype,
            label: fs.label,
            uuid: fs.uuid,
            mountpoint,
        }
    }
}

impl From<blk_device::Partition> for block_device::Partition {
    fn from(p: blk_device::Partition) -> Self {
        Self {
            parent: p.parent,
            number: p.number,
            name: p.name,
            scheme: p.scheme,
            typeid: p.typeid,
            uuid: p.uuid,
        }
    }
}

impl From<resource::Usage> for ResourceUsage {
    fn from(usage: resource::Usage) -> Self {
        let rusage = usage.0;
        ResourceUsage {
            soft_faults: rusage.ru_minflt,
            hard_faults: rusage.ru_majflt,
            swaps: rusage.ru_nswap,
            in_block_ops: rusage.ru_inblock,
            out_block_ops: rusage.ru_oublock,
            ipc_msg_send: rusage.ru_msgsnd,
            ipc_msg_rcv: rusage.ru_msgrcv,
            signals: rusage.ru_nsignals,
            vol_csw: rusage.ru_nvcsw,
            invol_csw: rusage.ru_nivcsw,
        }
    }
}

impl From<NvmeControllerInfo> for NvmeController {
    fn from(n: NvmeControllerInfo) -> Self {
        Self {
            name: n.name,
            state: NvmeControllerState::from(n.state) as i32,
            size: n.size,
            blk_size: n.blk_size,
        }
    }
}

impl From<ControllerState> for NvmeControllerState {
    fn from(state: ControllerState) -> Self {
        match state {
            ControllerState::New => NvmeControllerState::New,
            ControllerState::Initializing => NvmeControllerState::Initializing,
            ControllerState::Running => NvmeControllerState::Running,
            ControllerState::Faulted(_) => NvmeControllerState::Faulted,
            ControllerState::Unconfiguring => {
                NvmeControllerState::Unconfiguring
            }
            ControllerState::Unconfigured => NvmeControllerState::Unconfigured,
        }
    }
}

impl From<BlockDeviceIoStats> for NvmeControllerIoStats {
    fn from(b: BlockDeviceIoStats) -> Self {
        Self {
            num_read_ops: b.num_read_ops,
            num_write_ops: b.num_write_ops,
            bytes_read: b.bytes_read,
            bytes_written: b.bytes_written,
            num_unmap_ops: b.num_unmap_ops,
            bytes_unmapped: b.bytes_unmapped,
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

#[tonic::async_trait]
impl mayastor_server::Mayastor for MayastorSvc {
    #[named]
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();

                if args.disks.is_empty() {
                    return Err(Status::invalid_argument("Missing devices"));
                }

                let rx = rpc_submit::<_, _, LvsError>(async move {
                    let pool =
                        match Lvs::create_or_import(PoolArgs::try_from(args)?)
                            .await
                        {
                            Ok(p) => p,
                            // this check is added specifically so that the
                            // create_pool is idempotent
                            Err(LvsError::PoolCreate {
                                source,
                                name,
                            }) if source == Errno::EEXIST => {
                                info!(
                                    "returning already created pool {}",
                                    name,
                                );
                                Lvs::lookup(name.as_str())
                                    .expect("Already exists")
                            }
                            Err(e) => return Err(e),
                        };
                    // Capture current pool config and export to file.
                    PoolConfig::capture().export().await;
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
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                info!("{:?}", args);
                let rx = rpc_submit::<_, _, LvsError>(async move {
                    if let Some(pool) = Lvs::lookup(&args.name) {
                        // Remove pool from current config and export to file.
                        // Do this BEFORE we actually destroy the pool.
                        let mut config = PoolConfig::capture();
                        config.delete(&args.name);
                        config.export().await;

                        pool.destroy().await?;
                    }
                    Ok(Null {})
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
        request: Request<Null>,
    ) -> GrpcResult<ListPoolsReply> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, LvsError>(async move {
                    Ok(ListPoolsReply {
                        pools: Lvs::iter()
                            .map(|l| l.into())
                            .collect::<Vec<Pool>>(),
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

    #[named]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit(async move {
                    let args = request.into_inner();

                    if Lvs::lookup(&args.pool).is_none() {
                        return Err(LvsError::Invalid {
                            source: Errno::ENOSYS,
                            msg: format!("Pool {} not found", args.pool),
                        });
                    }

                    if let Some(b) = UntypedBdev::lookup_by_name(&args.uuid) {
                        let lvol = Lvol::try_from(b)?;
                        return Ok(Replica::from(lvol));
                    }

                    if !matches!(
                        Protocol::try_from(args.share)?,
                        Protocol::Off | Protocol::Nvmf
                    ) {
                        return Err(LvsError::ReplicaShareProtocol {
                            value: args.share,
                        });
                    }

                    let p = Lvs::lookup(&args.pool).unwrap();
                    match p
                        .create_lvol(&args.uuid, args.size, None, args.thin)
                        .await
                    {
                        Ok(mut lvol)
                            if Protocol::try_from(args.share)?
                                == Protocol::Nvmf =>
                        {
                            let props = ShareProps::new()
                                .with_allowed_hosts(args.allowed_hosts)
                                .with_ptpl(lvol.ptpl().create().map_err(
                                    |source| LvsError::LvolShare {
                                        source: crate::core::CoreError::Ptpl {
                                            reason: source.to_string(),
                                        },
                                        name: lvol.name(),
                                    },
                                )?);
                            match Pin::new(&mut lvol)
                                .share_nvmf(Some(props))
                                .await
                            {
                                Ok(s) => {
                                    debug!(
                                        "Created and shared {:?} as {}",
                                        lvol, s
                                    );
                                    Ok(Replica::from(lvol))
                                }
                                Err(e) => {
                                    debug!(
                                "Failed to share created {:?}: {} (destroying)",
                                lvol,
                                e.to_string()
                            );
                                    let _ = lvol.destroy().await;
                                    Err(e)
                                }
                            }
                        }
                        Ok(lvol) => {
                            debug!("Created {:?}", lvol);
                            Ok(Replica::from(lvol))
                        }
                        Err(e) => Err(e),
                    }
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
    async fn create_replica_v2(
        &self,
        request: Request<CreateReplicaRequestV2>,
    ) -> GrpcResult<ReplicaV2> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit(async move {
                    let args = request.into_inner();

                    let lvs = match Lvs::lookup(&args.pool) {
                        Some(lvs) => lvs,
                        None => {
                            return Err(LvsError::Invalid {
                                source: Errno::ENOSYS,
                                msg: format!("Pool {} not found", args.pool),
                            })
                        }
                    };

                    if let Some(b) = UntypedBdev::lookup_by_name(&args.name) {
                        let lvol = Lvol::try_from(b)?;
                        return Ok(ReplicaV2::from(lvol));
                    }

                    if !matches!(
                        Protocol::try_from(args.share)?,
                        Protocol::Off | Protocol::Nvmf
                    ) {
                        return Err(LvsError::ReplicaShareProtocol {
                            value: args.share,
                        });
                    }

                    match lvs
                        .create_lvol(
                            &args.name,
                            args.size,
                            Some(&args.uuid),
                            args.thin,
                        )
                        .await
                    {
                        Ok(mut lvol)
                            if Protocol::try_from(args.share)?
                                == Protocol::Nvmf =>
                        {
                            let props = ShareProps::new()
                                .with_allowed_hosts(args.allowed_hosts)
                                .with_ptpl(lvol.ptpl().create().map_err(
                                    |source| LvsError::LvolShare {
                                        source: crate::core::CoreError::Ptpl {
                                            reason: source.to_string(),
                                        },
                                        name: lvol.name(),
                                    },
                                )?);
                            match Pin::new(&mut lvol)
                                .share_nvmf(Some(props))
                                .await
                            {
                                Ok(s) => {
                                    debug!(
                                        "Created and shared {:?} as {}",
                                        lvol, s
                                    );
                                    Ok(ReplicaV2::from(lvol))
                                }
                                Err(e) => {
                                    debug!(
                                "Failed to share created {:?}: {} (destroying)",
                                lvol,
                                e.to_string()
                            );
                                    let _ = lvol.destroy().await;
                                    Err(e)
                                }
                            }
                        }
                        Ok(lvol) => {
                            debug!("Created {:?}", lvol);
                            Ok(ReplicaV2::from(lvol))
                        }
                        Err(e) => Err(e),
                    }
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
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<Null> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async {
            let args = request.into_inner();
            let rx = rpc_submit::<_, _, LvsError>(async move {
                if let Some(bdev) = UntypedBdev::lookup_by_name(&args.uuid) {
                    let lvol = Lvol::try_from(bdev)?;
                    lvol.destroy().await?;
                }
                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn list_replicas(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListReplicasReply> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async {
            let rx = rpc_submit::<_, _, LvsError>(async move {
                let mut replicas = Vec::new();
                if let Some(bdev) = UntypedBdev::bdev_first() {
                    replicas = bdev
                        .into_iter()
                        .filter(|b| b.driver() == "lvol")
                        .map(|b| Replica::from(Lvol::try_from(b).unwrap()))
                        .collect();
                }

                Ok(ListReplicasReply {
                    replicas,
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
    async fn list_replicas_v2(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListReplicasReplyV2> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async {
            let rx = rpc_submit::<_, _, LvsError>(async move {
                let mut replicas = Vec::new();
                if let Some(bdev) = UntypedBdev::bdev_first() {
                    replicas = bdev
                        .into_iter()
                        .filter(|b| b.driver() == "lvol")
                        .map(|b| ReplicaV2::from(Lvol::try_from(b).unwrap()))
                        .collect();
                }

                Ok(ListReplicasReplyV2 {
                    replicas,
                })
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    // TODO; lost track of what this is supposed to do
    async fn stat_replicas(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<StatReplicasReply> {
        let rx = rpc_submit::<_, _, CoreError>(async {
            let mut lvols = Vec::new();
            if let Some(bdev) = UntypedBdev::bdev_first() {
                bdev.into_iter()
                    .filter(|b| b.driver() == "lvol")
                    .for_each(|b| lvols.push(Lvol::try_from(b).unwrap()))
            }

            let mut replicas = Vec::new();
            for l in lvols {
                let stats = l.as_bdev().stats_async().await;
                if stats.is_err() {
                    error!("failed to get stats for {:?}", l);
                }

                replicas.push(ReplicaStats {
                    uuid: l.name(),
                    pool: l.pool_name(),
                    stats: stats.ok().map(Stats::from),
                });
            }

            Ok(StatReplicasReply {
                replicas,
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    #[named]
    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<ShareReplicaReply> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit(async move {
                    match UntypedBdev::lookup_by_name(&args.uuid) {
                        Some(bdev) => {
                            let mut lvol = Lvol::try_from(bdev)?;

                            // if we are already shared ...
                            if lvol.shared()
                                == Some(Protocol::try_from(args.share)?)
                            {
                                Pin::new(&mut lvol)
                                    .update_properties(
                                        UpdateProps::new().with_allowed_hosts(
                                            args.allowed_hosts,
                                        ),
                                    )
                                    .await?;
                                return Ok(ShareReplicaReply {
                                    uri: lvol.share_uri().unwrap(),
                                });
                            }

                            let mut lvol = Pin::new(&mut lvol);
                            match Protocol::try_from(args.share)? {
                                Protocol::Off => {
                                    lvol.as_mut().unshare().await?;
                                }
                                Protocol::Nvmf => {
                                    let props = ShareProps::new()
                                        .with_allowed_hosts(args.allowed_hosts)
                                        .with_ptpl(lvol.ptpl().create().map_err(
                                            |source| LvsError::LvolShare {
                                                source: crate::core::CoreError::Ptpl {
                                                    reason: source.to_string(),
                                                },
                                                name: lvol.name(),
                                            },
                                        )?);
                                    lvol.as_mut().share_nvmf(Some(props)).await?;
                                }
                            }

                            Ok(ShareReplicaReply {
                                uri: lvol.share_uri().unwrap(),
                            })
                        }

                        None => Err(LvsError::InvalidBdev {
                            source: BdevError::BdevNotFound {
                                name: args.uuid.clone(),
                            },
                            name: args.uuid,
                        }),
                    }
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
    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> GrpcResult<Nexus> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), true, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let uuid = args.uuid.clone();
                let name = uuid_to_name(&args.uuid)?;
                nexus::nexus_create(
                    &name,
                    args.size,
                    Some(&args.uuid),
                    &args.children,
                )
                .await?;
                let nexus = nexus_lookup(&uuid)?;
                info!("Created nexus: '{}'", uuid);
                Ok(nexus.to_grpc().await)
            })?;
            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn create_nexus_v2(
        &self,
        request: Request<CreateNexusV2Request>,
    ) -> GrpcResult<Nexus> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), true, async move {
            let resv_type = NvmeReservationConv(args.resv_type).try_into()?;
            let preempt_policy =
                NvmePreemptionConv(args.preempt_policy).try_into()?;
            // If the control plane has supplied a key, use it to store the
            // NexusInfo.
            let nexus_info_key = if args.nexus_info_key.is_empty() {
                None
            } else {
                Some(args.nexus_info_key.to_string())
            };

            let rx = rpc_submit::<_, _, nexus::Error>(async move {
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
                let nexus = nexus_lookup(&args.name)?;
                info!("Created nexus '{}'", &args.name);
                Ok(nexus.to_grpc().await)
            })?;
            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        info!("{:?}", ctx);
        self.serialized(ctx, args.uuid.clone(), true, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                nexus_destroy(&args.uuid).await?;
                Ok(Null {})
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
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);

                nexus_lookup(&args.uuid)?.shutdown().await?;
                Ok(Null {})
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
        request: Request<Null>,
    ) -> GrpcResult<ListNexusReply> {
        let args = request.into_inner();
        trace!("{:?}", args);

        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            Ok(ListNexusReply {
                nexus_list: {
                    let mut nexus_list =
                        Vec::with_capacity(nexus::nexus_iter().count());
                    for n in nexus::nexus_iter() {
                        if n.state.lock().deref() != &nexus::NexusState::Init {
                            nexus_list.push(n.to_grpc().await);
                        }
                    }
                    nexus_list
                },
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn list_nexus_v2(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListNexusV2Reply> {
        let args = request.into_inner();
        trace!("{:?}", args);

        let rx = rpc_submit::<_, _, nexus::Error>(async move {
            let mut nexus_list: Vec<NexusV2> = Vec::new();

            for n in nexus::nexus_iter() {
                if n.state.lock().deref() != &nexus::NexusState::Init {
                    nexus_list.push(n.to_grpc_v2().await);
                }
            }

            Ok(ListNexusV2Reply {
                nexus_list,
            })
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
    ) -> GrpcResult<Child> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let uuid = args.uuid.clone();
                debug!("Adding child {} to nexus {} ...", args.uri, uuid);
                let child = nexus_add_child(args).await?;
                info!("Added child to nexus {}", uuid);
                Ok(child)
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let uuid = args.uuid.clone();
                debug!("Removing child {} from nexus {} ...", args.uri, uuid);
                nexus_lookup(&args.uuid)?.remove_child(&args.uri).await?;
                info!("Removed child from nexus {}", uuid);
                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn fault_nexus_child(
        &self,
        request: Request<FaultNexusChildRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let uuid = args.uuid.clone();
                let uri = args.uri.clone();
                debug!("Faulting child {} on nexus {}", uri, uuid);
                nexus_lookup(&args.uuid)?
                    .fault_child(&args.uri, FaultReason::OfflinePermanent)
                    .await?;
                info!("Faulted child {} on nexus {}", uri, uuid);
                Ok(Null {})
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
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let uuid = args.uuid.clone();
                let uri = args.uri.clone();
                debug!("Injecting fault to nexus '{}': '{}'", uuid, uri);
                nexus_lookup(&args.uuid)?
                    .inject_add_fault(&args.uri)
                    .await?;
                info!("Injected fault to nexus '{}': '{}'", uuid, uri);
                Ok(Null {})
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
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let uuid = args.uuid.clone();
                let uri = args.uri.clone();
                debug!(
                    "Removing injected fault to nexus '{}': '{}'",
                    uuid, uri
                );
                nexus_lookup(&args.uuid)?
                    .inject_remove_fault(&args.uri)
                    .await?;
                info!("Removed injected fault to nexus '{}': '{}'", uuid, uri);
                Ok(Null {})
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
    ) -> GrpcResult<PublishNexusReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
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
                            sp_value: args.share,
                        });
                    }
                };

                let device_uri = nexus_lookup(&args.uuid)?
                    .share_ext(share_protocol, key, args.allowed_hosts.clone())
                    .await?;

                info!(
                    "Published nexus {} under {} for {:?}",
                    uuid, device_uri, args.allowed_hosts
                );
                Ok(PublishNexusReply {
                    device_uri,
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
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                let uuid = args.uuid.clone();
                debug!("Unpublishing nexus {} ...", uuid);
                nexus_lookup(&args.uuid)?.unshare_nexus().await?;
                info!("Unpublished nexus {}", uuid);
                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn get_nvme_ana_state(
        &self,
        request: Request<GetNvmeAnaStateRequest>,
    ) -> GrpcResult<GetNvmeAnaStateReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let uuid = args.uuid.clone();
                debug!("Getting NVMe ANA state for nexus {} ...", uuid);
                let ana_state =
                    nexus_lookup(&args.uuid)?.get_ana_state().await?;
                info!("Got nexus {} NVMe ANA state {:?}", uuid, ana_state);
                Ok(GetNvmeAnaStateReply {
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
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let uuid = args.uuid.clone();
                debug!("Setting NVMe ANA state for nexus {} ...", uuid);
                let ana_state = nexus::NvmeAnaState::from_i32(args.ana_state)?;

                let ana_state =
                    nexus_lookup(&args.uuid)?.set_ana_state(ana_state).await?;
                info!("Set nexus {} NVMe ANA state {:?}", uuid, ana_state);
                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);

                let nexus = nexus_lookup(&args.uuid)?;

                match args.action {
                    0 => {
                        nexus.fault_child(&args.uri, FaultReason::Offline).await
                    }
                    1 => nexus.online_child(&args.uri).await,
                    2 => {
                        nexus.fault_child(&args.uri, FaultReason::IoError).await
                    }
                    _ => Err(nexus::Error::InvalidKey {}),
                }?;

                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            trace!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&args.uuid)?
                    .start_rebuild(&args.uri)
                    .await
                    .map(|_| {})?;
                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            trace!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&args.uuid)?.stop_rebuild(&args.uri).await?;

                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let msg = request.into_inner();

        self.serialized(ctx, msg.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&msg.uuid)?.pause_rebuild(&msg.uri).await?;

                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> GrpcResult<Null> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let msg = request.into_inner();

        self.serialized(ctx, msg.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&msg.uuid)?.resume_rebuild(&msg.uri).await?;
                Ok(Null {})
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> GrpcResult<RebuildStateReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                trace!("{:?}", args);
                nexus_lookup(&args.uuid)?
                    .rebuild_state(&args.uri)
                    .map(RebuildStateReply::from)
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
    ) -> GrpcResult<RebuildStatsReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            trace!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&args.uuid)?
                    .rebuild_stats(&args.uri)
                    .await
                    .map(RebuildStatsReply::from)
            })?;
            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    #[named]
    async fn get_rebuild_progress(
        &self,
        request: Request<RebuildProgressRequest>,
    ) -> GrpcResult<RebuildProgressReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            trace!("{:?}", args);
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                nexus_lookup(&args.uuid)?
                    .rebuild_progress(&args.uri)
                    .await
                    .map(|p| RebuildProgressReply {
                        progress: p,
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
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> GrpcResult<CreateSnapshotReply> {
        let ctx = GrpcClientContext::new(&request, function_name!());
        let args = request.into_inner();

        self.serialized(ctx, args.uuid.clone(), false, async move {
            let rx = rpc_submit::<_, _, nexus::Error>(async move {
                let uuid = args.uuid.clone();
                debug!("Creating snapshot on nexus {} ...", uuid);

                let nexus = nexus_lookup(&args.uuid)?;
                let name = Uuid::new_v4().to_string(); // unique snapshot name

                let snapshot = SnapshotParams::new(
                    Some(args.uuid.clone()),
                    Some(args.uuid.clone()),
                    Some(Uuid::new_v4().to_string()), // unique tx id
                    Some(name.clone()),
                    None, // snapshot UUID will be handled on per-replica base.
                    Some(Utc::now().to_string()),
                );

                let mut replicas = Vec::new();
                for r in nexus.children() {
                    match r.get_uuid() {
                        Some(u) => {
                            replicas.push(
                                NexusReplicaSnapshotDescriptor {
                                    replica_uuid: u.to_string(),
                                    snapshot_uuid: Some(Uuid::new_v4().to_string()), // unique snapshot UUID.
                                    skip: false,
                                }
                            )
                        }
                        None => {
                            let emsg =
                                format!("Replica {} has no UUID", r.uri());
                            error!(
                                nexus = uuid,
                                error = emsg,
                                "Failed to create a snapshot for nexus"
                            );
                            return Err(NexusError::FailedCreateSnapshot {
                                name,
                                reason: emsg,
                            });
                        }
                    };
                }

                let reply = nexus
                    .create_snapshot(snapshot, replicas)
                    .await
                    .map(|_r| CreateSnapshotReply {
                        name,
                    })?;
                info!("Created snapshot on nexus {}", uuid);
                trace!("{:?}", reply);
                Ok(reply)
            })?;

            rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from)
                .map(Response::new)
        })
        .await
    }

    async fn list_block_devices(
        &self,
        request: Request<ListBlockDevicesRequest>,
    ) -> GrpcResult<ListBlockDevicesReply> {
        let args = request.into_inner();
        let block_devices = blk_device::list_block_devices(args.all).await?;

        let reply = ListBlockDevicesReply {
            devices: block_devices.into_iter().map(BlockDevice::from).collect(),
        };

        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    async fn get_resource_usage(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<GetResourceUsageReply> {
        let usage = resource::get_resource_usage().await?;
        let reply = GetResourceUsageReply {
            usage: Some(usage.into()),
        };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[named]
    async fn list_nvme_controllers(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListNvmeControllersReply> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    let controllers = list_controllers()
                        .await
                        .into_iter()
                        .map(NvmeController::from)
                        .collect();
                    Ok(ListNvmeControllersReply {
                        controllers,
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

    #[named]
    async fn stat_nvme_controllers(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<StatNvmeControllersReply> {
        self.locked(
            GrpcClientContext::new(&_request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, CoreError>(async move {
                    let mut res: Vec<NvmeControllerStats> = Vec::new();
                    let ctrls = list_controllers().await;
                    for ctrl in ctrls {
                        let stats = controller_stats(&ctrl.name).await;
                        if stats.is_ok() {
                            res.push(NvmeControllerStats {
                                name: ctrl.name,
                                stats: stats
                                    .ok()
                                    .map(NvmeControllerIoStats::from),
                            });
                        }
                    }
                    Ok(StatNvmeControllersReply {
                        controllers: res,
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

    async fn get_mayastor_info(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<MayastorInfoRequest> {
        let features = MayastorFeatures::get_features().into();

        let reply = MayastorInfoRequest {
            version: raw_version_string(),
            supported_features: Some(features),
        };

        Ok(Response::new(reply))
    }
}
