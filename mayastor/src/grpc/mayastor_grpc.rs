//! Mayastor grpc methods implementation.

use tonic::{Request, Response, Status};
use tracing::instrument;

use rpc::mayastor::*;

use crate::{
    bdev::{
        nexus::{instances, nexus_bdev},
        nexus_create,
    },
    core::Cores,
    grpc::{
        nexus_grpc::{
            nexus_add_child,
            nexus_destroy,
            nexus_lookup,
            uuid_to_name,
        },
        sync_config,
        GrpcResult,
    },
    pool,
    replica,
};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

#[derive(Debug)]
pub struct MayastorSvc {}

#[tonic::async_trait]
impl mayastor_server::Mayastor for MayastorSvc {
    #[instrument(level = "debug", err)]
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        sync_config(async {
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

            let pool = locally! { pool::create_pool(args) };

            info!("Created or imported pool {}", name);
            Ok(Response::new(pool))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let name = args.name.clone();
            debug!("Destroying pool {} ...", name);
            locally! { pool::destroy_pool(args) };
            info!("Destroyed pool {}", name);
            Ok(Response::new(Null {}))
        })
        .await
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
            pools: pool::PoolsIter::new().map(|p| p.into()).collect::<Vec<_>>(),
        };

        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Creating replica {} on {} ...", uuid, args.pool);
            let replica = locally! { replica::create_replica(args) };
            info!("Created replica {} ...", uuid);
            Ok(Response::new(replica))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Destroying replica {} ...", uuid);
            locally! { replica::destroy_replica(args) };
            info!("Destroyed replica {} ...", uuid);
            Ok(Response::new(Null {}))
        })
        .await
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
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Sharing replica {} ...", uuid);
            let reply = locally! { replica::share_replica(args) };
            info!("Shared replica {}", uuid);
            trace!("{:?}", reply);
            Ok(Response::new(reply))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> GrpcResult<Nexus> {
        sync_config( async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            let name = uuid_to_name(&args.uuid)?;
            debug!("Creating nexus {} ...", uuid);
            locally! { async move {
                nexus_create(&name, args.size, Some(&args.uuid), &args.children).await
            }};
            let nexus = nexus_lookup(&uuid)?;
            info!("Created nexus {}", uuid);
            Ok(Response::new(nexus.to_grpc()))
        }).await
    }

    #[instrument(level = "debug", err)]
    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Destroying nexus {} ...", uuid);
            locally! { async move {
                nexus_destroy(&args.uuid).await
            }};
            info!("Destroyed nexus {}", uuid);
            Ok(Response::new(Null {}))
        })
        .await
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
                .map(|n| n.to_grpc())
                .collect::<Vec<_>>(),
        };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    #[instrument(level = "debug", err)]
    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> GrpcResult<Child> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Adding child {} to nexus {} ...", args.uri, uuid);
            let child = locally! { async move {
                nexus_add_child(args).await
            }};
            info!("Added child to nexus {}", uuid);
            Ok(Response::new(child))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Removing child {} from nexus {} ...", args.uri, uuid);
            locally! { async move {
                nexus_lookup(&args.uuid)?.remove_child(&args.uri).await
            }};
            info!("Removed child from nexus {}", uuid);
            Ok(Response::new(Null {}))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> GrpcResult<PublishNexusReply> {
        sync_config(async {
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

            let share_protocol = match ShareProtocolNexus::from_i32(args.share)
            {
                Some(protocol) => protocol,
                None => {
                    return Err(nexus_bdev::Error::InvalidShareProtocol {
                        sp_value: args.share as i32,
                    }
                    .into())
                }
            };

            let device_uri = locally! { async move {
                nexus_lookup(&args.uuid)?.share(share_protocol, key).await
            }};

            info!("Published nexus {} under {}", uuid, device_uri);
            Ok(Response::new(PublishNexusReply {
                device_uri,
            }))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            debug!("Unpublishing nexus {} ...", uuid);
            locally! { async move {
                nexus_lookup(&args.uuid)?.unshare().await
            }};
            info!("Unpublished nexus {}", uuid);
            Ok(Response::new(Null {}))
        })
        .await
    }

    #[instrument(level = "debug", err)]
    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
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
        })
        .await
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
