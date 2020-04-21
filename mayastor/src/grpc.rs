use tonic::{transport::Server, Request, Response, Status};

use rpc::{
    mayastor::*,
    service::mayastor_server::{Mayastor, MayastorServer},
};

use crate::{
    bdev::{
        nexus::{instances, nexus_bdev, nexus_bdev::Nexus},
        nexus_create,
    },
    core::{Cores, Reactors},
    pool,
    replica,
};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

fn nexus_lookup(
    uuid: &str,
) -> std::result::Result<&mut Nexus, nexus_bdev::Error> {
    if let Some(nexus) = instances().iter_mut().find(|n| n.name == uuid) {
        Ok(nexus)
    } else {
        Err(nexus_bdev::Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

pub struct MayastorGrpc {}

type Result<T> = std::result::Result<T, Status>;

macro_rules! locally {
    ($body:expr) => {{
        let hdl = Reactors::current().spawn_local($body);
        hdl.await.unwrap()?
    }};
}

#[tonic::async_trait]
impl Mayastor for MayastorGrpc {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<Null>> {
        locally! { pool::create_pool(request.into_inner()) };
        Ok(Response::new(Null {}))
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<Response<Null>> {
        locally! { pool::destroy_pool(request.into_inner()) };
        Ok(Response::new(Null {}))
    }

    async fn list_pools(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListPoolsReply>> {
        assert_eq!(Cores::current(), Cores::first());

        let pools = pool::list_pools()
            .iter()
            .map(|pool| Pool {
                name: pool.name.clone(),
                disks: pool.disks.clone(),
                state: match pool.state.as_str() {
                    "online" => PoolState::PoolOnline,
                    _ => PoolState::PoolDegraded,
                } as i32,
                capacity: pool.capacity,
                used: pool.used,
            })
            .collect();

        dbg!(&pools);

        Ok(Response::new(ListPoolsReply {
            pools,
        }))
    }

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaReply>> {
        Ok(Response::new(
            locally! { replica::create_replica(request.into_inner()) },
        ))
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> Result<Response<Null>> {
        locally! { replica::destroy_replica(request.into_inner()) };
        Ok(Response::new(Null {}))
    }

    async fn list_replicas(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListReplicasReply>> {
        Ok(Response::new(replica::list_replicas()))
    }

    async fn stat_replicas(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<StatReplicasReply>> {
        Ok(Response::new(locally! { replica::stat_replicas() }))
    }

    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> Result<Response<ShareReplicaReply>> {
        Ok(Response::new(
            locally! { replica::share_replica(request.into_inner()) },
        ))
    }

    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
                nexus_create(&msg.uuid, msg.size, Some(&msg.uuid), &msg.children).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> Result<Response<Null>> {
        locally! { async move { nexus_lookup(&request.into_inner().uuid)?.destroy().await }};
        Ok(Response::new(Null {}))
    }

    async fn list_nexus(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListNexusReply>> {
        let list = instances()
            .iter()
            .map(|n| rpc::mayastor::Nexus {
                uuid: n.name.clone(),
                size: n.size,
                state: n.state.to_string(),
                device_path: n.get_share_path().unwrap_or_default(),
                children: n
                    .children
                    .iter()
                    .map(|c| Child {
                        uri: c.name.clone(),
                        state: c.state.to_string(),
                    })
                    .collect::<Vec<_>>(),
                rebuilds: n.rebuilds.len() as u64,
            })
            .collect::<Vec<_>>();

        Ok(Response::new(ListNexusReply {
            nexus_list: list,
        }))
    }
    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
            nexus_lookup(&msg.uuid)?.add_child(&msg.uri).await.map(|_| ())
        }};

        Ok(Response::new(Null {}))
    }

    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
            nexus_lookup(&msg.uuid)?.remove_child(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> Result<Response<PublishNexusReply>> {
        let msg = request.into_inner();

        if msg.key != "" && msg.key.len() != 16 {
            return Err(nexus_bdev::Error::InvalidKey {}.into());
        }

        let key: Option<String> = if msg.key == "" {
            None
        } else {
            Some(msg.key.clone())
        };

        let share_protocol = match ShareProtocolNexus::from_i32(msg.share) {
            Some(protocol) => protocol,
            None => {
                return Err(nexus_bdev::Error::InvalidShareProtocol {
                    sp_value: msg.share as i32,
                }
                .into())
            }
        };

        let reply = locally! { async move {
            nexus_lookup(&msg.uuid)?.share(share_protocol, key).await.map(|device_path| PublishNexusReply { device_path })
        }};

        Ok(Response::new(reply))
    }

    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> Result<Response<Null>> {
        locally! { async move {
            nexus_lookup(&request.into_inner().uuid)?.unshare().await
        }};

        Ok(Response::new(Null {}))
    }

    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();

        let onl = match msg.action {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(Status::invalid_argument("Bad child operation")),
        }?;

        locally! { async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            if onl {
                nexus.online_child(&msg.uri).await
            } else {
                nexus.offline_child(&msg.uri).await
            }
        }};

        Ok(Response::new(Null {}))
    }

    async fn start_rebuild(
        &self,
        request: Request<StartRebuildRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
            nexus_lookup(&msg.uuid)?.start_rebuild_rpc(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn stop_rebuild(
        &self,
        request: Request<StopRebuildRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        locally! { async move {
          nexus_lookup(&msg.uuid)?.stop_rebuild(&msg.uri).await
        }};

        Ok(Response::new(Null {}))
    }

    async fn get_rebuild_state(
        &self,
        request: Request<RebuildStateRequest>,
    ) -> Result<Response<RebuildStateReply>> {
        let msg = request.into_inner();

        Ok(Response::new(locally! { async move {
            nexus_lookup(&msg.uuid)?.get_rebuild_state(&msg.uri).await
        }}))
    }

    async fn get_rebuild_progress(
        &self,
        request: Request<RebuildProgressRequest>,
    ) -> Result<Response<RebuildProgressReply>> {
        let msg = request.into_inner();

        Ok(Response::new(locally! { async move {
            nexus_lookup(&msg.uuid)?.get_rebuild_progress().await
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
