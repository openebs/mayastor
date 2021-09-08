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
        nexus::{instances, nexus_bdev},
        nexus_create,
        nexus_create_v2,
        Reason,
    },
    core::{
        Bdev,
        BlockDeviceIoStats,
        CoreError,
        MayastorFeatures,
        Protocol,
        Share,
    },
    grpc::{
        controller_grpc::{controller_stats, list_controllers},
        mayastor_grpc::nexus_bdev::NexusNvmeParams,
        nexus_grpc::{
            nexus_add_child,
            nexus_destroy,
            nexus_lookup,
            uuid_to_name,
        },
        rpc_submit,
        GrpcClientContext,
        GrpcResult,
        Serializer,
    },
    host::{blk_device, resource},
    lvm::{
        pool::{VolGroup, MAYASTOR_LABEL},
        volume::{LogicalVolume},
        Error as LvmError,
    },
    lvs::{Error as LvsError, Lvol, Lvs},
    nexus_uri::{bdev_create, bdev_destroy, NexusBdevError},
    subsys::{PoolBackend, PoolConfig},
};
use futures::FutureExt;
use nix::errno::Errno;
use rpc::mayastor::*;
use std::{convert::TryFrom, fmt::Debug, ops::Deref, time::Duration};
use tonic::{Request, Response, Status};
#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

use ::function_name::named;
use git_version::git_version;
use std::panic::AssertUnwindSafe;

impl GrpcClientContext {
    #[track_caller]
    pub fn new<T>(req: &Request<T>, fid: &str) -> Self
    where
        T: Debug,
    {
        Self {
            args: format!("{:?}", req.get_ref()),
            id: fid.to_string(),
        }
    }
}

#[derive(Debug)]
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
}

impl From<LvsError> for Status {
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
            LvsError::ReplicaShareProtocol {
                ..
            } => Status::invalid_argument(e.to_string()),

            LvsError::Destroy {
                source, ..
            } => source.into(),
            LvsError::Invalid {
                ..
            } => Status::invalid_argument(e.to_string()),
            LvsError::InvalidBdev {
                source, ..
            } => source.into(),
            _ => Status::internal(e.to_string()),
        }
    }
}

impl From<LvmError> for Status {
    fn from(e: LvmError) -> Self {
        match e {
            LvmError::InvalidPoolType {
                ..
            }
            | LvmError::Io {
                ..
            } => Status::invalid_argument(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}

impl From<Protocol> for i32 {
    fn from(p: Protocol) -> Self {
        match p {
            Protocol::Off => 0,
            Protocol::Nvmf => 1,
            Protocol::Iscsi => 2,
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
            pooltype: PoolType::Lvs as i32,
        }
    }
}

impl From<VolGroup> for Pool {
    fn from(v: VolGroup) -> Self {
        Self {
            name: v.name().into(),
            disks: v.disks(),
            state: PoolState::PoolOnline.into(),
            capacity: v.capacity(),
            used: v.used(),
            pooltype: PoolType::Lvm as i32,
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
            pool: l.pool(),
            thin: l.is_thin(),
            size: l.size(),
            share: l.shared().unwrap().into(),
            uri: l.share_uri().unwrap(),
            pooltype: PoolType::Lvs as i32,
        }
    }
}

impl From<Lvol> for ReplicaV2 {
    fn from(l: Lvol) -> Self {
        Self {
            name: l.name(),
            uuid: l.uuid(),
            pool: l.pool(),
            thin: l.is_thin(),
            size: l.size(),
            share: l.shared().unwrap().into(),
            uri: l.share_uri().unwrap(),
        }
    }
}

impl From<LogicalVolume> for Replica {
    fn from(l: LogicalVolume) -> Self {
        Self {
            // name is used as uuid also, although we can use lv_uuid
            uuid: l.name().into(),
            pool: l.vg_name().into(),
            // not supporting thin pool at the moment
            thin: false,
            size: l.size(),
            share: l.share(),
            //lv_path is used as uri
            uri: l.lv_path().into(),
            pooltype: PoolType::Lvm as i32,
        }
    }
}

impl From<MayastorFeatures> for rpc::mayastor::MayastorFeatures {
    fn from(f: MayastorFeatures) -> Self {
        Self {
            asymmetric_namespace_access: f.asymmetric_namespace_access,
            lvm: f.lvm,
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

                let resp = match PoolBackend::try_from(args.pooltype)? {
                    PoolBackend::Lvs => {
                        if MayastorFeatures::get_features().lvm {
                            // check if a lvm pool already exists with the same name
                            if let Some(pool) = VolGroup::lookup_by_name(args.name.as_str(), MAYASTOR_LABEL).await {
                                return Err(Status::invalid_argument(format!("lvm pool with the name '{}' already exists", pool.name())))
                            };
                            // check if the disks are used by existing lvm pool
                            if let Some(pool) = VolGroup::lookup_by_disk(args.disks[0].as_str()).await {
                                return Err(Status::invalid_argument(format!("a lvm pool {} already uses the disks {:?}", pool.name(), pool.disks())))
                            };
                        }
                        let rx = rpc_submit::<_, _, LvsError>(async move {
                            let pool = Lvs::create_or_import(args).await?;
                            // Capture current pool config and export to file.
                            PoolConfig::capture().export().await;
                            Ok(Pool::from(pool))
                        })?;
                        rx.await
                            .map_err(|_| Status::cancelled("cancelled"))?
                            .map_err(Status::from)
                            .map(Response::new)
                    },
                    PoolBackend::Lvm => {
                        if !MayastorFeatures::get_features().lvm {
                            return Err(Status::failed_precondition("lvm support not available"))
                        }
                        // check if a lvs pool already exists with the same name
                        if let Some(_pool) = Lvs::lookup(args.name.as_str()) {
                            return Err(Status::invalid_argument("lvs pool with the same name already exists"))
                        };
                        // check if the disks are used by existing lvs pool
                        if Lvs::iter()
                            .map(|l| l.base_bdev().name()).any(|d| args.disks.contains(&d)){
                                return Err(Status::invalid_argument("a lvs pool already uses the disk"))
                            };
                        let res = VolGroup::import_or_create(args).await
                            .map_err(Status::from)
                            .map(Pool::from)
                            .map(Response::new);
                        let volumes = LogicalVolume::list("").await
                            .map_err(Status::from)?;
                        for volume in volumes.iter() {
                            let _res = create_and_share_bdev(volume.lv_path(), volume.share()).await;
                        }
                        res
                    },
                };
                resp
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
                let mut lvm_pool_found = false;
                if MayastorFeatures::get_features().lvm {
                    let res: Result<_, LvmError> = {
                        if let Some(pool) =
                            VolGroup::lookup_by_name(&args.name, MAYASTOR_LABEL)
                                .await
                        {
                            lvm_pool_found = true;
                            pool.destroy().await?;
                        }
                        Ok(Null {})
                    };
                    if lvm_pool_found {
                        return res.map_err(Status::from).map(Response::new);
                    }
                }

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
                    Ok(Lvs::iter().map(|l| l.into()).collect::<Vec<Pool>>())
                })?;

                let rec = rx
                    .await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from);

                let mut lvs_pools = match rec {
                    Ok(pools) => pools,
                    Err(e) => return Err(e),
                };

                if MayastorFeatures::get_features().lvm {
                    let mut lvm_pools =
                        match VolGroup::list(MAYASTOR_LABEL).await {
                            Ok(pools) => pools
                                .iter()
                                .map(|v| v.clone().into())
                                .collect::<Vec<Pool>>(),
                            Err(e) => {
                                error!("failed to fetch lvm pools {}", e);
                                vec![]
                            }
                        };
                    lvs_pools.append(&mut lvm_pools);
                }

                Ok(Response::new(ListPoolsReply {
                    pools: lvs_pools,
                }))
            },
        )
        .await
    }

    #[named]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async move {

            let args = request.into_inner();

            if !matches!(
                            Protocol::try_from(args.share)?,
                            Protocol::Off | Protocol::Nvmf
                        ) {
                return Err(LvsError::ReplicaShareProtocol {
                    value: args.share,
                }).map_err(Status::from);
            }

            match PoolBackend::try_from(args.pooltype)? {
                PoolBackend::Lvs => {
                    let rx = rpc_submit(async move {
                        if Lvs::lookup(&args.pool).is_none() {
                            return Err(LvsError::Invalid {
                                source: Errno::ENOSYS,
                                msg: format!("Pool {} not found", args.pool),
                            });
                        }

                        if let Some(b) = Bdev::lookup_by_name(&args.uuid) {
                            let lvol = Lvol::try_from(b)?;
                            return Ok(Replica::from(lvol));
                        }

                        let p = Lvs::lookup(&args.pool).unwrap();
                        match p.create_lvol(&args.uuid, args.size, None, false).await {
                            Ok(lvol)
                            if Protocol::try_from(args.share)? == Protocol::Nvmf =>
                                {
                                    match lvol.share_nvmf(None).await {
                                        Ok(s) => {
                                            debug!("created and shared {} as {}", lvol, s);
                                            Ok(Replica::from(lvol))
                                        }
                                        Err(e) => {
                                            debug!(
                                "failed to share created lvol {}: {} (destroying)",
                                lvol,
                                e.to_string()
                            );
                                            let _ = lvol.destroy().await;
                                            Err(e)
                                        }
                                    }
                                }
                            Ok(lvol) => {
                                debug!("created lvol {}", lvol);
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
                PoolBackend::Lvm => {

                    if !MayastorFeatures::get_features().lvm {
                        return Err(Status::failed_precondition("lvm support not available"))
                    }

                    if let None = VolGroup::lookup_by_name(args.pool.as_str(), MAYASTOR_LABEL).await {
                        return Err(Status::invalid_argument(format!("lvm pool {} does not exist", args.pool)))
                    };

                    if let Some(replica) = LogicalVolume::lookup_by_lv_name(args.uuid.as_str().to_string()).await {
                        return Ok(Response::new(Replica::from(replica)))
                    }

                    let vol_uuid =  args.uuid.as_str().to_string();
                    let share_protocol = args.share;

                    let vol = LogicalVolume::create(args).await
                        .map_err(|e| {
                            println!("{:#?}", e);
                            LvsError::RepCreate {
                                source: Errno::UnknownErrno,
                                name: vol_uuid,
                            }
                        })
                        .map_err(Status::from)?;

                    create_and_share_bdev(&vol.lv_path(), share_protocol).await
                        .map(|b| {
                            let mut replica = Replica::from(vol.clone());
                            replica.uri = b;
                            replica
                        })
                        .map_err(|e| {
                            let _r = vol.remove();
                            e
                        })
                        .map(Response::new)

                }
            }
        }).await
    }

    #[named]
    async fn create_replica_v2(
        &self,
        request: Request<CreateReplicaRequestV2>,
    ) -> GrpcResult<ReplicaV2> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async move {
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

            if let Some(b) = Bdev::lookup_by_name(&args.name) {
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

            match lvs.create_lvol(&args.name, args.size, Some(&args.uuid), false).await {
                Ok(lvol)
                    if Protocol::try_from(args.share)? == Protocol::Nvmf =>
                {
                    match lvol.share_nvmf(None).await {
                        Ok(s) => {
                            debug!("created and shared {} as {}", lvol, s);
                            Ok(ReplicaV2::from(lvol))
                        }
                        Err(e) => {
                            debug!(
                                "failed to share created lvol {}: {} (destroying)",
                                lvol,
                                e.to_string()
                            );
                            let _ = lvol.destroy().await;
                            Err(e)
                        }
                    }
                }
                Ok(lvol) => {
                    debug!("created lvol {}", lvol);
                    Ok(ReplicaV2::from(lvol))
                }
                Err(e) => Err(e),
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
        }).await
    }

    #[named]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<Null> {
        self.locked(GrpcClientContext::new(&request, function_name!()), async {
            let args = request.into_inner();

            if let Some(replica) = LogicalVolume::lookup_by_lv_name(args.uuid.as_str().to_string()).await {

                let lv_path = replica.lv_path().to_owned();
                let share = replica.share();

                let rx1 = rpc_submit::<_, _, Status>(async move {

                    if let Some(bdev) = Bdev::lookup_by_name(lv_path.as_str()) {
                        if matches!(Protocol::try_from(share)?,
                            Protocol::Nvmf | Protocol::Iscsi) {
                            bdev.unshare().await?;
                            info!("unshared replica {}", lv_path);
                        }
                        bdev_destroy(bdev.bdev_uri().unwrap().as_str()).await?;
                        info!("destroyed bdev {}", lv_path);
                    }
                    Ok(Null{})
                })?;
                rx1.await.map_err(|_| Status::cancelled("cancelled"))?.map_err(Status::from)?;

                let _rs = replica.remove().await?;

                return Ok(Response::new(Null {}))
            }

            let rx2 = rpc_submit::<_, _, LvsError>(async move {
                if let Some(bdev) = Bdev::lookup_by_name(&args.uuid) {
                    let lvol = Lvol::try_from(bdev)?;
                    lvol.destroy().await?;
                }
                Ok(Null {})
            })?;

            rx2.await
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
                let mut lvs_replicas = Vec::new();
                if let Some(bdev) = Bdev::bdev_first() {
                    lvs_replicas = bdev
                        .into_iter()
                        .filter(|b| b.driver() == "lvol")
                        .map(|b| Replica::from(Lvol::try_from(b).unwrap()))
                        .collect();
                }

                Ok(lvs_replicas)
            })?;

            let r = rx.await
                .map_err(|_| Status::cancelled("cancelled"))?
                .map_err(Status::from);

            let mut replicas = match r {
                Ok(r) => r,
                Err(e) => return Err(e),
            };

            let lvm_replicas = LogicalVolume::list("").await
                .map_err(|e| LvsError::Invalid {
                    source: Errno::UnknownErrno,
                    msg: e.to_string(),
                })?;

            replicas.append(&mut lvm_replicas.into_iter().map(|r| Replica::from(r)).collect());

            Ok(ListReplicasReply {
                replicas,
            }).map(Response::new)

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
                if let Some(bdev) = Bdev::bdev_first() {
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
            if let Some(bdev) = Bdev::bdev_first() {
                bdev.into_iter()
                    .filter(|b| b.driver() == "lvol")
                    .for_each(|b| lvols.push(Lvol::try_from(b).unwrap()))
            }

            let mut replicas = Vec::new();
            for l in lvols {
                let stats = l.as_bdev().stats().await;
                if stats.is_err() {
                    error!("failed to get stats for lvol: {}", l);
                }

                replicas.push(ReplicaStats {
                    uuid: l.name(),
                    pool: l.pool(),
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
                let mut args = request.into_inner();

                if let Some(volume) = LogicalVolume::lookup_by_lv_name(args.uuid.as_str().to_string()).await {
                    let uuid = volume.name().to_string();
                    volume.change_share_tag(args.share).await?;
                    args.uuid = uuid;
                }

                let rx = rpc_submit(async move {
                    match Bdev::lookup_by_name(&args.uuid) {
                        Some(bdev) => {
                            let lvol = Lvol::try_from(bdev)?;

                            // if we are already shared ...
                            if lvol.shared()
                                == Some(Protocol::try_from(args.share)?)
                            {
                                return Ok(ShareReplicaReply {
                                    uri: lvol.share_uri().unwrap(),
                                });
                            }

                            match Protocol::try_from(args.share)? {
                                Protocol::Off => {
                                    lvol.unshare().await?;
                                }
                                Protocol::Nvmf => {
                                    lvol.share_nvmf(None).await?;
                                }
                                Protocol::Iscsi => {
                                    return Err(LvsError::LvolShare {
                                        source: CoreError::NotSupported {
                                            source: Errno::ENOSYS,
                                        },
                                        name: args.uuid,
                                    });
                                }
                            }

                            Ok(ShareReplicaReply {
                                uri: lvol.share_uri().unwrap(),
                            })
                        }

                        None => Err(LvsError::InvalidBdev {
                            source: NexusBdevError::BdevNotFound {
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
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    let uuid = args.uuid.clone();
                    let name = uuid_to_name(&args.uuid)?;
                    nexus_create(
                        &name,
                        args.size,
                        Some(&args.uuid),
                        &args.children,
                    )
                    .await?;
                    let nexus = nexus_lookup(&uuid)?;
                    info!("Created nexus {}", uuid);
                    Ok(nexus.to_grpc())
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
    async fn create_nexus_v2(
        &self,
        request: Request<CreateNexusV2Request>,
    ) -> GrpcResult<Nexus> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    nexus_create_v2(
                        &args.name,
                        args.size,
                        Some(&args.uuid),
                        NexusNvmeParams {
                            min_cntlid: args.min_cntl_id as u16,
                            max_cntlid: args.max_cntl_id as u16,
                            resv_key: args.resv_key,
                        },
                        &args.children,
                    )
                    .await?;
                    let nexus = nexus_lookup(&args.name)?;
                    info!("Created nexus {}", &args.name);
                    Ok(nexus.to_grpc())
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
    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    let args = request.into_inner();
                    trace!("{:?}", args);
                    nexus_destroy(&args.uuid).await?;
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

    async fn list_nexus(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListNexusReply> {
        let args = request.into_inner();
        trace!("{:?}", args);

        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            Ok(ListNexusReply {
                nexus_list: instances()
                    .iter()
                    .filter(|n| {
                        n.state.lock().deref() != &nexus_bdev::NexusState::Init
                    })
                    .map(|n| n.to_grpc())
                    .collect::<Vec<_>>(),
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

        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            let mut nexus_list: Vec<NexusV2> = Vec::new();

            for n in instances() {
                if n.state.lock().deref() != &nexus_bdev::NexusState::Init {
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

    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> GrpcResult<Child> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
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
    }

    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> GrpcResult<Null> {
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            let args = request.into_inner();
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
    }

    async fn fault_nexus_child(
        &self,
        request: Request<FaultNexusChildRequest>,
    ) -> GrpcResult<Null> {
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            let uri = args.uri.clone();
            debug!("Faulting child {} on nexus {}", uri, uuid);
            nexus_lookup(&args.uuid)?
                .fault_child(&args.uri, Reason::Rpc)
                .await?;
            info!("Faulted child {} on nexus {}", uri, uuid);
            Ok(Null {})
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> GrpcResult<PublishNexusReply> {
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Publishing nexus {} ...", uuid);

            if !args.key.is_empty() && args.key.len() != 16 {
                return Err(nexus_bdev::Error::InvalidKey {});
            }

            let key: Option<String> = if args.key.is_empty() {
                None
            } else {
                Some(args.key.clone())
            };

            let share_protocol = match ShareProtocolNexus::from_i32(args.share)
            {
                Some(protocol) => protocol,
                None => {
                    return Err(nexus_bdev::Error::InvalidShareProtocol {
                        sp_value: args.share as i32,
                    });
                }
            };

            let device_uri =
                nexus_lookup(&args.uuid)?.share(share_protocol, key).await?;

            info!("Published nexus {} under {}", uuid, device_uri);
            Ok(PublishNexusReply {
                device_uri,
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
    ) -> GrpcResult<Null> {
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async {
            let args = request.into_inner();
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
    }

    async fn get_nvme_ana_state(
        &self,
        request: Request<GetNvmeAnaStateRequest>,
    ) -> GrpcResult<GetNvmeAnaStateReply> {
        let args = request.into_inner();
        let uuid = args.uuid.clone();
        debug!("Getting NVMe ANA state for nexus {} ...", uuid);

        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            let ana_state = nexus_lookup(&args.uuid)?.get_ana_state().await?;
            info!("Got nexus {} NVMe ANA state {:?}", uuid, ana_state);
            Ok(GetNvmeAnaStateReply {
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
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        let uuid = args.uuid.clone();
        debug!("Setting NVMe ANA state for nexus {} ...", uuid);

        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            let ana_state = match NvmeAnaState::from_i32(args.ana_state) {
                Some(ana_state) => ana_state,
                None => {
                    return Err(nexus_bdev::Error::InvalidNvmeAnaState {
                        ana_value: args.ana_state as i32,
                    });
                }
            };

            let ana_state =
                nexus_lookup(&args.uuid)?.set_ana_state(ana_state).await?;
            info!("Set nexus {} NVMe ANA state {:?}", uuid, ana_state);
            Ok(Null {})
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    #[named]
    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    let args = request.into_inner();
                    trace!("{:?}", args);

                    let onl = match args.action {
                        1 => Ok(true),
                        0 => Ok(false),
                        _ => Err(nexus_bdev::Error::InvalidKey {}),
                    }?;

                    let nexus = nexus_lookup(&args.uuid)?;
                    if onl {
                        nexus.online_child(&args.uri).await?;
                    } else {
                        nexus.offline_child(&args.uri).await?;
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
    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
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
            },
        )
        .await
    }

    #[named]
    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    nexus_lookup(&args.uuid)?.stop_rebuild(&args.uri).await?;

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
    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let msg = request.into_inner();
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    nexus_lookup(&msg.uuid)?.pause_rebuild(&msg.uri).await?;

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
    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> GrpcResult<Null> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let msg = request.into_inner();
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    nexus_lookup(&msg.uuid)?.resume_rebuild(&msg.uri).await?;
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
    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> GrpcResult<RebuildStateReply> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    trace!("{:?}", args);
                    nexus_lookup(&args.uuid)?.get_rebuild_state(&args.uri).await
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
    ) -> GrpcResult<RebuildStatsReply> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    nexus_lookup(&args.uuid)?.get_rebuild_stats(&args.uri).await
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
    async fn get_rebuild_progress(
        &self,
        request: Request<RebuildProgressRequest>,
    ) -> GrpcResult<RebuildProgressReply> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                trace!("{:?}", args);
                let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
                    nexus_lookup(&args.uuid)?.get_rebuild_progress(&args.uri)
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(Response::new)
            },
        )
        .await
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> GrpcResult<CreateSnapshotReply> {
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async {
            let args = request.into_inner();
            let uuid = args.uuid.clone();
            debug!("Creating snapshot on nexus {} ...", uuid);
            let reply = nexus_lookup(&args.uuid)?.create_snapshot().await?;
            info!("Created snapshot on nexus {}", uuid);
            trace!("{:?}", reply);
            Ok(reply)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn list_block_devices(
        &self,
        request: Request<ListBlockDevicesRequest>,
    ) -> GrpcResult<ListBlockDevicesReply> {
        let args = request.into_inner();
        let reply = ListBlockDevicesReply {
            devices: blk_device::list_block_devices(args.all).await?,
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
            usage: Some(usage),
        };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    async fn list_nvme_controllers(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<ListNvmeControllersReply> {
        list_controllers().await
    }

    async fn stat_nvme_controllers(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<StatNvmeControllersReply> {
        controller_stats().await
    }

    async fn get_mayastor_info(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<MayastorInfoRequest> {
        let features = MayastorFeatures::get_features().into();

        let reply = MayastorInfoRequest {
            version: git_version!(
                args = ["--tags", "--abbrev=12"],
                fallback = "unknown"
            )
            .to_string(),
            supported_features: Some(features),
        };

        Ok(Response::new(reply))
    }
}

async fn create_and_share_bdev(uri: &str, share_protocol: i32) -> Result<String, Status> {
    let uri = format!("uring://{}", uri);
    let rx = rpc_submit(async move {

        match bdev_create(uri.as_str()).await {
            Ok(b)
            // no need to check for error here, as the share protocol is already validated
            if Protocol::try_from(share_protocol).unwrap() == Protocol::Nvmf => {
                let bdev = Bdev::lookup_by_name(&b).unwrap();
                let share = bdev.share_nvmf(None).await?;
                let bdev = Bdev::lookup_by_name(&b).unwrap();
                Ok(bdev.share_uri().unwrap_or(share))
            },
            Ok(b) => Ok(b),
            Err(e) => Err(e).map_err(Status::from),
        }
    })?;

    rx.await.map_err(|_| Status::cancelled("cancelled"))?
        .map_err(Status::from)
}
