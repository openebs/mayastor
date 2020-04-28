use tonic::{transport::Server, Request, Response, Status};

use rpc::{
    mayastor::*,
    service::mayastor_server::{Mayastor, MayastorServer},
};

use crate::{
    bdev::{
        nexus::{
            instances,
            nexus_bdev,
            nexus_bdev::{
                name_to_uuid,
                uuid_to_name,
                Nexus,
                NexusState as NexusStateInternal,
            },
            nexus_child::ChildState as ChildStateInternal,
        },
        nexus_create,
    },
    core::{Cores, Reactors},
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

pub struct MayastorGrpc {}

type Result<T> = std::result::Result<T, Status>;

fn print_error_chain(err: &dyn std::error::Error) -> String {
    let mut msg = format!("{}", err);
    let mut opt_source = err.source();
    while let Some(source) = opt_source {
        msg = format!("{}: {}", msg, source);
        opt_source = source.source();
    }
    msg
}

/// Macro locally is used to spawn a future on the same thread that executes
/// the macro. That is needed to guarantee that the execution context does
/// not leave the mgmt core (core0) that is a basic assumption in spdk. Also
/// the input/output parameters don't have to be Send and Sync in that case,
/// which simplifies the code. The value of the macro is Ok() variant of the
/// expression in the macro. Err() variant returns from the function.
macro_rules! locally {
    ($body:expr) => {{
        let hdl = Reactors::current().spawn_local($body);
        match hdl.await.unwrap() {
            Ok(res) => res,
            Err(err) => {
                error!("{}", print_error_chain(&err));
                return Err(err.into());
            }
        }
    }};
}

#[tonic::async_trait]
impl Mayastor for MayastorGrpc {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<Null>> {
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

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<Response<Null>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let name = args.name.clone();
        debug!("Destroying pool {} ...", name);
        locally! { pool::destroy_pool(args) };
        info!("Destroyed pool {}", name);
        Ok(Response::new(Null {}))
    }

    async fn list_pools(
        &self,
        request: Request<Null>,
    ) -> Result<Response<ListPoolsReply>> {
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

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Creating replica {} on {} ...", uuid, args.pool);
        let reply = locally! { replica::create_replica(args) };
        info!("Created replica {} ...", uuid);
        Ok(Response::new(reply))
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> Result<Response<Null>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Destroying replica {} ...", uuid);
        locally! { replica::destroy_replica(args) };
        info!("Destroyed replica {} ...", uuid);
        Ok(Response::new(Null {}))
    }

    async fn list_replicas(
        &self,
        request: Request<Null>,
    ) -> Result<Response<ListReplicasReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        assert_eq!(Cores::current(), Cores::first());
        let reply = replica::list_replicas();
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    async fn stat_replicas(
        &self,
        request: Request<Null>,
    ) -> Result<Response<StatReplicasReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let reply = locally! { replica::stat_replicas() };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> Result<Response<ShareReplicaReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Sharing replica {} ...", uuid);
        let reply = locally! { replica::share_replica(args) };
        info!("Shared replica {}", uuid);
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> Result<Response<Null>> {
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

    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> Result<Response<Null>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Destroying nexus {} ...", uuid);
        locally! { async move { nexus_lookup(&args.uuid)?.destroy().await }};
        info!("Destroyed nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    async fn list_nexus(
        &self,
        request: Request<Null>,
    ) -> Result<Response<ListNexusReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let reply = ListNexusReply {
            nexus_list: instances()
                .iter()
                .map(|n| rpc::mayastor::Nexus {
                    uuid: name_to_uuid(&n.name).to_string(),
                    size: n.size,
                    state: match n.state {
                        NexusStateInternal::Online => NexusState::NexusOnline,
                        NexusStateInternal::Faulted => NexusState::NexusFaulted,
                        NexusStateInternal::Degraded => {
                            NexusState::NexusDegraded
                        }
                        NexusStateInternal::Init => NexusState::NexusDegraded,
                        NexusStateInternal::Closed => NexusState::NexusDegraded,
                    } as i32,
                    device_path: n.get_share_path().unwrap_or_default(),
                    children: n
                        .children
                        .iter()
                        .map(|c| Child {
                            uri: c.name.clone(),
                            state: match c.state {
                                ChildStateInternal::Init => {
                                    ChildState::ChildDegraded
                                }
                                ChildStateInternal::ConfigInvalid => {
                                    ChildState::ChildFaulted
                                }
                                ChildStateInternal::Open => {
                                    ChildState::ChildOnline
                                }
                                // Treating closed as rebuild is the most safe
                                // as we don't
                                // want moac to do anything if a child is being
                                // closed.
                                ChildStateInternal::Closed => {
                                    ChildState::ChildDegraded
                                }
                                ChildStateInternal::Faulted => {
                                    ChildState::ChildFaulted
                                }
                            } as i32,
                            rebuild_progress: 0,
                        })
                        .collect::<Vec<_>>(),
                    rebuilds: RebuildJob::count() as u64,
                })
                .collect::<Vec<_>>(),
        };
        trace!("{:?}", reply);
        Ok(Response::new(reply))
    }

    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> Result<Response<Null>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        let uuid = args.uuid.clone();
        debug!("Adding child {} to nexus {} ...", args.uri, uuid);
        locally! { async move {
            nexus_lookup(&args.uuid)?.add_child(&args.uri).await.map(|_| ())
        }};
        info!("Added child to nexus {}", uuid);
        Ok(Response::new(Null {}))
    }

    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> Result<Response<Null>> {
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

    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> Result<Response<PublishNexusReply>> {
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

    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> Result<Response<Null>> {
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

    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> Result<Response<Null>> {
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

    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> Result<Response<Null>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        locally! { async move {
            nexus_lookup(&args.uuid)?.start_rebuild_rpc(&args.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> Result<Response<Null>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        locally! { async move {
          nexus_lookup(&args.uuid)?.stop_rebuild(&args.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn pause_rebuild(
        &self,
        request: Request<PauseRebuildRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
          nexus_lookup(&msg.uuid)?.pause_rebuild(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn resume_rebuild(
        &self,
        request: Request<ResumeRebuildRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
          nexus_lookup(&msg.uuid)?.resume_rebuild(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> Result<Response<RebuildStateReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        Ok(Response::new(locally! { async move {
            nexus_lookup(&args.uuid)?.get_rebuild_state(&args.uri).await
        }}))
    }

    async fn get_rebuild_progress(
        &self,
        request: Request<RebuildProgressRequest>,
    ) -> Result<Response<RebuildProgressReply>> {
        let args = request.into_inner();
        trace!("{:?}", args);
        Ok(Response::new(locally! { async move {
            nexus_lookup(&args.uuid)?.get_rebuild_progress(&args.uri)
        }}))
    }
}

pub async fn grpc_server_init(
    addr: &str,
    port: &str,
) -> std::result::Result<(), ()> {
    info!("gRPC server configured at address '{}:{}'", addr, port);
    let saddr = format!("{}:{}", addr, port).parse().unwrap();
    let svc = Server::builder()
        .add_service(MayastorServer::new(MayastorGrpc {}))
        .serve(saddr);

    match svc.await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("gRPC server failed with error: {}", e);
            Err(())
        }
    }
}
