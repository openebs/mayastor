//! Mayastor grpc methods implementation.
//!
//! The Mayastor gRPC methods serve as a higher abstraction for provisioning
//! replicas and targets to be used with CSI.
//!
//! We want to keep the code here to a minimal, for example grpc/pool.rs
//! contains all the conversions and mappings etc to whatever interface from a
//! grpc perspective we provide. Also, by doing his, we can test the methods
//! without the need for setting up a grpc client.

use tonic::{Request, Response, Status};
use tracing::instrument;

use rpc::mayastor::*;

use crate::{
    bdev::{
        nexus::{instances, nexus_bdev},
        nexus_create,
        Reason,
    },
    grpc::{
        nexus_grpc::{
            nexus_add_child,
            nexus_destroy,
            nexus_lookup,
            uuid_to_name,
        },
        pool_grpc,
        sync_config,
        GrpcResult,
    },
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
        let args = request.into_inner();

        if args.disks.is_empty() {
            return Err(Status::invalid_argument("Missing devices"));
        }

        sync_config(pool_grpc::create(args)).await
    }

    #[instrument(level = "debug", err)]
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        sync_config(pool_grpc::destroy(args)).await
    }

    #[instrument(level = "debug", err)]
    async fn list_pools(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<ListPoolsReply> {
        pool_grpc::list()
    }

    #[instrument(level = "debug", err)]
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> GrpcResult<Replica> {
        let args = request.into_inner();
        sync_config(pool_grpc::create_replica(args)).await
    }

    #[instrument(level = "debug", err)]
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> GrpcResult<Null> {
        let args = request.into_inner();
        sync_config(pool_grpc::destroy_replica(args)).await
    }

    #[instrument(level = "debug", err)]
    async fn list_replicas(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<ListReplicasReply> {
        pool_grpc::list_replicas()
    }

    #[instrument(level = "debug", err)]
    // TODO; lost track of what this is supposed to do
    async fn stat_replicas(
        &self,
        _request: Request<Null>,
    ) -> GrpcResult<StatReplicasReply> {
        pool_grpc::stat_replica().await
    }

    #[instrument(level = "debug", err)]
    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> GrpcResult<ShareReplicaReply> {
        let args = request.into_inner();
        sync_config(pool_grpc::share_replica(args)).await
    }

    #[instrument(level = "debug", err)]
    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> GrpcResult<Nexus> {
        sync_config(async {
            let args = request.into_inner();
            let uuid = args.uuid.clone();
            let name = uuid_to_name(&args.uuid)?;
            locally! { async move {
                nexus_create(&name, args.size, Some(&args.uuid), &args.children).await
            }}
            ;
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
            locally! { async move {
                nexus_destroy(&args.uuid).await
            }};
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
    async fn fault_nexus_child(
        &self,
        request: Request<FaultNexusChildRequest>,
    ) -> GrpcResult<Null> {
        sync_config(async {
            let args = request.into_inner();
            trace!("{:?}", args);
            let uuid = args.uuid.clone();
            let uri = args.uri.clone();
            debug!("Faulting child {} on nexus {}", uri, uuid);
            locally! { async move {
                nexus_lookup(&args.uuid)?.fault_child(&args.uri, Reason::Rpc).await
            }};
            info!("Faulted child {} on nexus {}", uri, uuid);
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
                    .into());
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
                nexus_lookup(&args.uuid)?.unshare_nexus().await
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

    #[instrument(level = "debug", err)]
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> GrpcResult<CreateSnapshotReply> {
        sync_config(async {
            let args = request.into_inner();
            let uuid = args.uuid.clone();
            debug!("Creating snapshot on nexus {} ...", uuid);
            let reply = locally! { async move {
                nexus_lookup(&args.uuid)?.create_snapshot().await
            }};
            info!("Created snapshot on nexus {}", uuid);
            trace!("{:?}", reply);
            Ok(Response::new(reply))
        })
        .await
    }
}
