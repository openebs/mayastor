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
};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

fn nexus_lookup(
    uuid: &str,
) -> Result<&mut Nexus, crate::bdev::nexus::nexus_bdev::Error> {
    if let Some(nexus) = instances().iter_mut().find(|n| &n.name == uuid) {
        Ok(nexus)
    } else {
        Err(crate::bdev::nexus::nexus_bdev::Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

pub struct MayastorGrpc {}

#[tonic::async_trait]
impl Mayastor for MayastorGrpc {
    async fn create_pool(
        &self,
        _request: Request<CreatePoolRequest>,
    ) -> Result<Response<Null>, Status> {
        todo!()
    }

    async fn destroy_pool(
        &self,
        _request: Request<DestroyPoolRequest>,
    ) -> Result<Response<Null>, Status> {
        todo!()
    }

    async fn list_pools(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListPoolsReply>, Status> {
        assert_eq!(Cores::current(), Cores::first());

        let pools = crate::pool::list_pools();

        dbg!(pools);

        Ok(Response::new(ListPoolsReply {
            pools: Vec::new(),
        }))
    }

    async fn create_replica(
        &self,
        _request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaReply>, Status> {
        todo!()
    }
    async fn destroy_replica(
        &self,
        _request: Request<DestroyReplicaRequest>,
    ) -> Result<Response<Null>, Status> {
        todo!()
    }

    async fn list_replicas(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListReplicasReply>, Status> {
        todo!()
    }

    async fn stat_replicas(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<StatReplicasReply>, Status> {
        todo!()
    }

    async fn share_replica(
        &self,
        _request: Request<ShareReplicaRequest>,
    ) -> Result<Response<ShareReplicaReply>, Status> {
        todo!()
    }

    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> Result<Response<Null>, Status> {
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
    ) -> Result<Response<Null>, Status> {
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
    ) -> Result<Response<ListNexusReply>, Status> {
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
        _request: Request<AddChildNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        todo!()
    }

    async fn remove_child_nexus(
        &self,
        _request: Request<RemoveChildNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        todo!()
    }

    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> Result<Response<PublishNexusReply>, Status> {
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
    ) -> Result<Response<Null>, Status> {
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
        _request: Request<ChildNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        todo!()
    }
}

pub async fn grpc_server_init() -> Result<(), ()> {
    let saddr = "192.168.1.4:1234".to_string().parse().unwrap();
    let svc = Server::builder()
        .add_service(MayastorServer::new(MayastorGrpc {}))
        .serve(saddr);

    match svc.await {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}
