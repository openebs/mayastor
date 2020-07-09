use std::convert::From;

use tonic::{Request, Response, Status};
use tracing::instrument;

use rpc::{mayastor::*, service::mayastor_server::Mayastor};

use crate::{
    bdev::{
        nexus::{
            instances,
            nexus_bdev,
            nexus_bdev::{name_to_uuid, uuid_to_name, Nexus, NexusStatus},
            nexus_child::{ChildStatus, NexusChild},
        },
        nexus_create,
    },
    core::Cores,
    grpc::GrpcResult,
    pool,
    rebuild::RebuildJob,
    replica,
};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

/// Lookup a nexus by its uuid prepending "nexus-" prefix. Return error if
/// uuid is invalid or nexus not found.
fn nexus_lookup(
    uuid: &str,
) -> std::result::Result<&mut Nexus, nexus_bdev::Error> {
    let name = uuid_to_name(uuid)?;

    if let Some(nexus) = instances().iter_mut().find(|n| n.name == name) {
        Ok(nexus)
    } else {
        Err(nexus_bdev::Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

impl From<ChildStatus> for ChildState {
    fn from(child: ChildStatus) -> Self {
        match child {
            ChildStatus::Faulted => ChildState::ChildFaulted,
            ChildStatus::Degraded => ChildState::ChildDegraded,
            ChildStatus::Online => ChildState::ChildOnline,
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

impl From<&NexusChild> for Child {
    fn from(child: &NexusChild) -> Self {
        Child {
            uri: child.name.clone(),
            state: ChildState::from(child.status()) as i32,
            rebuild_progress: child.get_rebuild_progress(),
        }
    }
}

#[derive(Debug)]
pub struct MayastorSvc {}

#[tonic::async_trait]
impl Mayastor for MayastorSvc {
    #[instrument(level = "debug", err)]
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let name = args.name.clone();

        if args.disks.is_empty() {
            return Err(Status::invalid_argument("Missing devices"));
        }
        debug!(
            "Creating pool {} on {} with block size {}, io_if {}...",
            name,
            args.disks.join(" "),
            args.block_size,
            args.io_if,
        );

        locally! { pool::create_pool(args) };

        info!("Created or imported pool {}", name);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let name = args.name.clone();
        debug!("Destroying pool {} ...", name);
        locally! { pool::destroy_pool(args) };
        info!("Destroyed pool {}", name);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn list_pools(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListPoolsReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        assert_eq!(Cores::current(), Cores::first());

        let reply = ListPoolsReply {
            pools: pool::list_pools()
                .iter()
                .map(|pool| Pool {
                    name: pool.name.clone(),
                    disks: pool.disks.clone(),
                    state: match pool.state.as_str() {
                        "online" => PoolState::PoolOnline,
                        _ => PoolState::PoolFaulted,
                    } as i32,
                    capacity: pool.capacity,
                    used: pool.used,
                })
                .collect(),
        };

        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<CreateReplicaReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Creating replica {} on {} ...", uuid, args.pool);
        let reply = locally! { replica::create_replica(args) };
        info!("Created replica {} ...", uuid);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Destroying replica {} ...", uuid);
        locally! { replica::destroy_replica(args) };
        info!("Destroyed replica {} ...", uuid);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn list_replicas(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListReplicasReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        assert_eq!(Cores::current(), Cores::first());
        let reply = replica::list_replicas();
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn stat_replicas(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<StatReplicasReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let reply = locally! { replica::stat_replicas() };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<ShareReplicaReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Sharing replica {} ...", uuid);
        let reply = locally! { replica::share_replica(args) };
        info!("Shared replica {}", uuid);
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        let name = uuid_to_name(&args.uuid)?;
        debug!("Creating nexus {} ...", uuid);
        locally! { async move {
            nexus_create(&name, args.size, Some(&args.uuid), &args.children).await
        }};
        info!("Created nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Destroying nexus {} ...", uuid);
        locally! { async move { nexus_lookup(&args.uuid)?.destroy().await }};
        info!("Destroyed nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn list_nexus(
        &self,
        request: Request<Null>,
    ) -> GrpcResult<ListNexusReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let reply = ListNexusReply {
            nexus_list: instances()
                .iter()
                .map(|n| rpc::mayastor::Nexus {
                    uuid: name_to_uuid(&n.name).to_string(),
                    size: n.size,
                    state: NexusState::from(n.status()) as i32,
                    device_path: n.get_share_path().unwrap_or_default(),
                    children: n
                        .children
                        .iter()
                        .map(Child::from)
                        .collect::<Vec<_>>(),
                    rebuilds: RebuildJob::count() as u32,
                })
                .collect::<Vec<_>>(),
        };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Adding child {} to nexus {} ...", args.uri, uuid);
        locally! { async move {
            nexus_lookup(&args.uuid)?.add_child(&args.uri, args.norebuild).await.map(|_| ())
        }};
        info!("Added child to nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Removing child {} from nexus {} ...", args.uri, uuid);
        locally! { async move {
            nexus_lookup(&args.uuid)?.remove_child(&args.uri).await
        }};
        info!("Removed child from nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> GrpcResult<PublishNexusReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Publishing nexus {} ...", uuid);

        if args.key != "" && args.key.len() != 16 {
            return Err(nexus_bdev::Error::InvalidKey {}.into());
        }

        let key: Option<String> = if args.key.is_empty() {
            None
        } else {
            Some(args.key.clone())
        };

        let share_protocol = match ShareProtocolNexus::from_i32(args.share) {
            Some(protocol) => protocol,
            None => {
                return Err(nexus_bdev::Error::InvalidShareProtocol {
                    sp_value: args.share as i32,
                }
                .into())
            }
        };

        let device_path = locally! { async move {
            nexus_lookup(&args.uuid)?.share(share_protocol, key).await
        }};

        info!("Published nexus {} under {}", uuid, device_path);
        Ok(Response::new(PublishNexusReply {
            device_path,
        }))
    }

    #[instrument(level = "debug", err)]
    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Unpublishing nexus {} ...", uuid);
        locally! { async move {
            nexus_lookup(&args.uuid)?.unshare().await
        }};
        info!("Unpublished nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);

        let onl = match args.action {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(Status::invalid_argument("Bad child operation")),
        }?;

        locally! { async move {
            let nexus = nexus_lookup(&args.uuid)?;
            if onl {
                nexus.online_child(&args.uri).await
            } else {
                nexus.offline_child(&args.uri).await
            }
        }};

        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        locally! { async move {
            nexus_lookup(&args.uuid)?.start_rebuild(&args.uri).await.map(|_|{})
        }};

        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        trace!("{:?}", args);
        locally! { async move {
          nexus_lookup(&args.uuid)?.stop_rebuild(&args.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> GrpcResult<Null> {
        let msg = request.into_inner();
        locally! { async move {
          nexus_lookup(&msg.uuid)?.pause_rebuild(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> GrpcResult<Null> {
        let msg = request.into_inner();
        locally! { async move {
          nexus_lookup(&msg.uuid)?.resume_rebuild(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    #[instrument(level = "debug", err)]
    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> GrpcResult<RebuildStateReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        Ok(Response::new(locally! { async move {
            nexus_lookup(&args.uuid)?.get_rebuild_state(&args.uri).await
        }}))
    }

    #[instrument(level = "debug", err)]
    async fn get_rebuild_progress(
        &self,
        request: Request<RebuildProgressRequest>,
    ) -> GrpcResult<RebuildProgressReply> {
        let args = request.into_inner();
        trace!("{:?}", args);
        Ok(Response::new(locally! { async move {
            nexus_lookup(&args.uuid)?.get_rebuild_progress(&args.uri)
        }}))
    }
}
