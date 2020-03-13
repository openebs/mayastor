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
    replica,
    pool,
};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

fn nexus_lookup(
    uuid: &str,
) -> std::result::Result<&mut Nexus, nexus_bdev::Error> {
    if let Some(nexus) = instances().iter_mut().find(|n| &n.name == uuid) {
        Ok(nexus)
    } else {
        Err(nexus_bdev::Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

pub struct MayastorGrpc {}

type Result<T> = std::result::Result<T, Status>;

#[tonic::async_trait]
impl Mayastor for MayastorGrpc {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        let hdl = Reactors::current().spawn_local(pool::create_pool(msg));
        hdl.await.unwrap()?;
        Ok(Response::new(Null {}))
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        let hdl = Reactors::current().spawn_local(pool::destroy_pool(msg));
        hdl.await.unwrap()?;
        Ok(Response::new(Null {}))
    }

    async fn list_pools(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListPoolsReply>> {
        assert_eq!(Cores::current(), Cores::first());

        let pools = pool::list_pools().iter().map(|pool| {
            Pool {
                name: pool.name.clone(),
                disks: pool.disks.clone(),
                state: match pool.state.as_str() {
                    "online" => PoolState::Online,
                    _ => PoolState::Degraded,
                } as i32,
                capacity: pool.capacity,
                used: pool.used,
            }
        }).collect();

        dbg!(&pools);

        Ok(Response::new(ListPoolsReply { pools }))
    }

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaReply>> {
        let msg = request.into_inner();
        let hdl = Reactors::current().spawn_local(replica::create_replica(msg));
        Ok(Response::new(hdl.await.unwrap()?))
    }

    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();
        let hdl = Reactors::current().spawn_local(replica::destroy_replica(msg));
        hdl.await.unwrap()?;
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
        let hdl = Reactors::current().spawn_local(replica::stat_replicas());
        Ok(Response::new(hdl.await.unwrap()?))
    }

    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> Result<Response<ShareReplicaReply>> {
        let msg = request.into_inner();
        let hdl = Reactors::current().spawn_local(async move {
            Result::<ShareReplicaReply>::Ok(replica::share_replica(msg).await?)
        });
        Ok(Response::new(hdl.await.unwrap()?))
    }

    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();

        let hdl = Reactors::current().spawn_local(async move {
            nexus_create(&msg.uuid, msg.size, Some(&msg.uuid), &msg.children)
                .await
        });

        // a handle returns an Option<Result<T,E>> so unwrap by default
        hdl.await.unwrap()?;

        Ok(Response::new(Null {}))
    }

    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();

        let hdl = Reactors::current().spawn_local(async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            nexus.destroy().await
        });

        hdl.await.unwrap()?;

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

        let hdl = Reactors::current().spawn_local(async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            nexus.add_child(&msg.uri).await.map(|_| ())
        });

        hdl.await.unwrap()?;

        Ok(Response::new(Null {}))
    }

    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();

        let hdl = Reactors::current().spawn_local(async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            nexus.remove_child(&msg.uri).await
        });

        hdl.await.unwrap()?;

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

        let hdl = Reactors::current().spawn_local(async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            nexus.share(key).await.map(|device_path| PublishNexusReply {
                device_path,
            })
        });

        let reply = hdl.await.unwrap()?;

        Ok(Response::new(reply))
    }

    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> Result<Response<Null>> {
        let msg = request.into_inner();

        let hdl = Reactors::current().spawn_local(async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            nexus.unshare().await
        });

        hdl.await.unwrap()?;
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
            _ => Err(Status::invalid_argument("Bad child operation"))
        }?;

        let hdl = Reactors::current().spawn_local(async move {
            let nexus = nexus_lookup(&msg.uuid)?;
            if onl {
                nexus.online_child(&msg.uri).await
            } else {
                nexus.offline_child(&msg.uri).await
            }
        });

        hdl.await.unwrap()?;

        Ok(Response::new(Null {}))
    }
}

pub async fn grpc_server_init() -> std::result::Result<(), ()> {
    let saddr = "127.0.0.1:1234".to_string().parse().unwrap();
    let svc = Server::builder()
        .add_service(MayastorServer::new(MayastorGrpc {}))
        .serve(saddr);

    match svc.await {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}
