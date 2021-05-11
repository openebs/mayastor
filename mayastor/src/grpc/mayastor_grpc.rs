//! Mayastor grpc methods implementation.
//!
//! The Mayastor gRPC methods serve as a higher abstraction for provisioning
//! replicas and targets to be used with CSI.
//!
//! We want to keep the code here to a minimal, for example grpc/pool.rs
//! contains all the conversions and mappings etc to whatever interface from a
//! grpc perspective we provide. Also, by doing his, we can test the methods
//! without the need for setting up a grpc client.

use crate::{
    bdev::{
        nexus::{instances, nexus_bdev},
        nexus_create,
        Reason,
    },
    core::{Bdev, BlockDeviceIoStats, CoreError, Protocol, Share},
    grpc::{
        nexus_grpc::{
            nexus_add_child,
            nexus_destroy,
            nexus_lookup,
            uuid_to_name,
        },
        rpc_submit,
        GrpcResult,
    },
    host::{blk_device, resource},
    lvs::{Error as LvsError, Lvol, Lvs},
    nexus_uri::NexusBdevError,
};
use nix::errno::Errno;
use rpc::mayastor::*;
use std::convert::TryFrom;
use tonic::{Request, Response, Status};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

#[derive(Debug)]
pub struct MayastorSvc;

impl From<LvsError> for Status {
    fn from(e: LvsError) -> Self {
        match e {
            LvsError::Import {
                ..
            } => Status::invalid_argument(e.to_string()),
            LvsError::Create {
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
        }
    }
}
#[tonic::async_trait]
impl mayastor_server::Mayastor for MayastorSvc {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        let args = request.into_inner();

        if args.disks.is_empty() {
            return Err(Status::invalid_argument("Missing devices"));
        }

        let rx = rpc_submit::<_, _, LvsError>(async move {
            let pool = Lvs::create_or_import(args).await?;

            Ok(Pool::from(pool))
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, LvsError>(async move {
            if let Some(pool) = Lvs::lookup(&args.name) {
                let _e = pool.destroy().await?;
            }

            Ok(Null {})
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn list_pools(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<ListPoolsReply> {
        let rx = rpc_submit::<_, _, LvsError>(async move {
            Ok(ListPoolsReply {
                pools: Lvs::iter().map(|l| l.into()).collect::<Vec<Pool>>(),
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let rx = rpc_submit(async move {
            let args = request.into_inner();
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

            if !matches!(
                Protocol::try_from(args.share)?,
                Protocol::Off | Protocol::Nvmf
            ) {
                return Err(LvsError::ReplicaShareProtocol {
                    value: args.share,
                });
            }

            let p = Lvs::lookup(&args.pool).unwrap();
            match p.create_lvol(&args.uuid, args.size, false).await {
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
                                "failed to share created lvol {}: {} .. destroying",
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
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        let rx = rpc_submit(async move {
            match Bdev::lookup_by_name(&args.uuid) {
                Some(b) => {
                    let lvol = Lvol::try_from(b)?;
                    lvol.destroy().await.map(|_r| Null {})
                }
                None => Ok(Null {}),
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn list_replicas(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<ListReplicasReply> {
        let rx = rpc_submit::<_, _, LvsError>(async move {
            let mut replicas = Vec::new();
            if let Some(bdev) = Bdev::bdev_first() {
                replicas = bdev
                    .into_iter()
                    .filter(|b| b.driver() == "lvol")
                    .map(|b| Replica::from(Lvol::try_from(b).unwrap()))
                    .collect::<Vec<_>>();
            }

            Ok(ListReplicasReply {
                replicas,
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
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

    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<ShareReplicaReply> {
        let args = request.into_inner();
        let rx = rpc_submit(async move {
            if let Some(b) = Bdev::lookup_by_name(&args.uuid) {
                let lvol = Lvol::try_from(b)?;

                // if we are already shared return OK
                if lvol.shared() == Some(Protocol::try_from(args.share)?) {
                    return Ok(ShareReplicaReply {
                        uri: lvol.share_uri().unwrap(),
                    });
                }
                match Protocol::try_from(args.share)? {
                    Protocol::Off => {
                        lvol.unshare().await.map(|_| ShareReplicaReply {
                            uri: lvol.share_uri().unwrap(),
                        })
                    }

                    Protocol::Nvmf => {
                        lvol.share_nvmf(None).await.map(|_| ShareReplicaReply {
                            uri: lvol.share_uri().unwrap(),
                        })
                    }
                    Protocol::Iscsi => Err(LvsError::LvolShare {
                        source: CoreError::NotSupported {
                            source: Errno::ENOSYS,
                        },
                        name: args.uuid,
                    }),
                }
            } else {
                Err(LvsError::InvalidBdev {
                    source: NexusBdevError::BdevNotFound {
                        name: args.uuid.clone(),
                    },
                    name: args.uuid,
                })
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> GrpcResult<Nexus> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            let uuid = args.uuid.clone();
            let name = uuid_to_name(&args.uuid)?;
            nexus_create(&name, args.size, Some(&args.uuid), &args.children)
                .await?;
            let nexus = nexus_lookup(&uuid)?;
            info!("Created nexus {}", uuid);
            Ok(nexus.to_grpc())
        })?;
        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<Null> {
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
                    .map(|n| n.to_grpc())
                    .collect::<Vec<_>>(),
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

    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> GrpcResult<Null> {
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
    }

    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> GrpcResult<Null> {
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
    }

    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> GrpcResult<Null> {
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
    }

    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> GrpcResult<Null> {
        let msg = request.into_inner();
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            nexus_lookup(&msg.uuid)?.pause_rebuild(&msg.uri).await?;

            Ok(Null {})
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> GrpcResult<Null> {
        let msg = request.into_inner();
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            nexus_lookup(&msg.uuid)?.resume_rebuild(&msg.uri).await?;
            Ok(Null {})
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> GrpcResult<RebuildStateReply> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            trace!("{:?}", args);
            nexus_lookup(&args.uuid)?.get_rebuild_state(&args.uri).await
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn get_rebuild_stats(
        &self,
        request: Request<RebuildStatsRequest>,
    ) -> GrpcResult<RebuildStatsReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            nexus_lookup(&args.uuid)?.get_rebuild_stats(&args.uri).await
        })?;
        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn get_rebuild_progress(
        &self,
        request: Request<RebuildProgressRequest>,
    ) -> GrpcResult<RebuildProgressReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
            nexus_lookup(&args.uuid)?.get_rebuild_progress(&args.uri)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
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
}
