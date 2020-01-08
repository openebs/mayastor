//! Implementation of gRPC methods from mayastor gRPC service.

use tonic::{Code, Request, Response, Status};

use rpc::jsonrpc as jsondata;

use crate::rpc::{mayastor::*, service};

/// mayastorService handles non CSI rpc calls
#[derive(Clone, Debug)]
pub struct MayastorService {
    pub socket: String,
}
impl MayastorService {}
#[tonic::async_trait]
impl service::mayastor_server::Mayastor for MayastorService {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();

        trace!("{:?}", msg);
        if msg.disks.is_empty() {
            return Err(Status::new(Code::InvalidArgument, "Missing devices"));
        }

        debug!(
            "Creating pool {} on {} with block size {}...",
            msg.name,
            msg.disks.join(" "),
            msg.block_size,
        );

        // make a copy of vars used in the closures below
        let pool_name = msg.name.clone();

        let args = Some(jsondata::CreateOrImportPoolArgs {
            name: msg.name,
            disks: msg.disks,
            block_size: Some(msg.block_size),
        });

        jsonrpc::call::<_, ()>(&self.socket, "create_or_import_pool", args)
            .await?;

        info!("Created or imported pool {}", pool_name);
        Ok(Response::new(Null {}))
    }

    /// Destroy pool -> destroy lvol store and delete underlying base bdev.
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        let args = Some(jsondata::DestroyPoolArgs {
            name: msg.name.clone(),
        });

        // make a copy of vars used in the closures below
        let socket = self.socket.clone();
        let pool_name = msg.name;

        debug!("Destroying pool {} ...", pool_name);

        jsonrpc::call::<_, ()>(&socket, "destroy_pool", args).await?;
        info!("Destroyed pool {}", pool_name);
        Ok(Response::new(Null {}))
    }

    /// Get list of lvol stores.
    ///
    /// TODO: There is a state field which is always set to "online" state.
    /// Figure out how to set it properly.
    async fn list_pools(
        &self,
        request: Request<Null>,
    ) -> Result<Response<ListPoolsReply>, Status> {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        let pools = jsonrpc::call::<(), Vec<jsondata::Pool>>(
            &self.socket,
            "list_pools",
            None,
        )
        .await?;

        debug!("Got list of {} pools", pools.len());
        let resp = Response::new(ListPoolsReply {
            pools: pools
                .iter()
                .map(|p| Pool {
                    name: p.name.clone(),
                    disks: p.disks.clone(),
                    capacity: p.capacity,
                    used: p.used,
                    state: match p.state.as_str() {
                        "online" => PoolState::Online,
                        "degraded" => PoolState::Degraded,
                        "faulty" => PoolState::Faulty,
                        _ => PoolState::Faulty,
                    } as i32,
                })
                .collect(),
        });
        trace!("{:?}", resp);
        Ok(resp)
    }

    /// Create replica
    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaReply>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        let uuid = msg.uuid.clone();
        let pool = msg.pool.clone();
        debug!("Creating replica {} on {} ...", uuid, pool);

        match jsonrpc::call::<_, CreateReplicaReply>(
            &self.socket,
            "create_replica",
            Some(msg),
        )
        .await
        {
            Ok(val) => Ok(Response::new(val)),
            Err(err) => {
                error!("{}", err);
                Err(err.into())
            }
        }
    }

    /// Destroy replica
    async fn destroy_replica(
        &self,
        request: Request<DestroyReplicaRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        let uuid = msg.uuid.clone();
        debug!("Destroying replica {} ...", uuid);

        match jsonrpc::call::<_, ()>(&self.socket, "destroy_replica", Some(msg))
            .await
        {
            Ok(_) => {
                info!("Destroyed replica {}", uuid);
                Ok(Response::new(Null {}))
            }
            Err(err) => {
                error!("{}", err);
                Err(err.into())
            }
        }
    }

    /// Share replica
    async fn share_replica(
        &self,
        request: Request<ShareReplicaRequest>,
    ) -> Result<Response<ShareReplicaReply>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        let uuid = msg.uuid.clone();
        debug!("Sharing replica {} ...", uuid);

        match jsonrpc::call::<_, ShareReplicaReply>(
            &self.socket,
            "share_replica",
            Some(msg),
        )
        .await
        {
            Ok(reply) => {
                info!("Shared replica {}", uuid);
                Ok(Response::new(reply))
            }
            Err(err) => {
                error!("{}", err);
                Err(err.into())
            }
        }
    }

    /// List replicas
    async fn list_replicas(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListReplicasReply>, Status> {
        let list = jsonrpc::call::<(), ListReplicasReply>(
            &self.socket,
            "list_replicas",
            None,
        )
        .await?;
        debug!("Got list of {} replicas", list.replicas.len());
        trace!("{:?}", list);
        Ok(Response::new(list))
    }

    /// Return replica stats
    async fn stat_replicas(
        &self,
        request: Request<Null>,
    ) -> Result<Response<StatReplicasReply>, Status> {
        let msg = request.into_inner();
        let socket = &self.socket;

        trace!("{:?}", msg);

        match jsonrpc::call::<(), StatReplicasReply>(
            socket,
            "stat_replicas",
            None,
        )
        .await
        {
            Ok(reply) => {
                let resp = Response::new(reply);
                trace!("{:?}", resp);
                Ok(resp)
            }
            Err(err) => {
                error!("{}", err);
                Err(err.into_status())
            }
        }
    }

    async fn create_nexus(
        &self,
        request: Request<CreateNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        jsonrpc::call::<_, ()>(&self.socket, "create_nexus", Some(msg)).await?;
        Ok(Response::new(Null {}))
    }

    async fn destroy_nexus(
        &self,
        request: Request<DestroyNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        jsonrpc::call::<_, ()>(&self.socket, "destroy_nexus", Some(msg))
            .await?;
        Ok(Response::new(Null {}))
    }

    async fn list_nexus(
        &self,
        _request: Request<Null>,
    ) -> Result<Response<ListNexusReply>, Status> {
        let list = jsonrpc::call::<(), ListNexusReply>(
            &self.socket,
            "list_nexus",
            None,
        )
        .await?;

        Ok(Response::new(list))
    }

    async fn publish_nexus(
        &self,
        request: Request<PublishNexusRequest>,
    ) -> Result<Response<PublishNexusReply>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        let p = jsonrpc::call::<_, PublishNexusReply>(
            &self.socket,
            "publish_nexus",
            Some(msg),
        )
        .await?;
        Ok(Response::new(p))
    }

    async fn unpublish_nexus(
        &self,
        request: Request<UnpublishNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        jsonrpc::call::<_, ()>(&self.socket, "unpublish_nexus", Some(msg))
            .await?;
        Ok(Response::new(Null {}))
    }

    async fn child_operation(
        &self,
        request: Request<ChildNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        jsonrpc::call::<_, ()>(&self.socket, "offline_child", Some(msg))
            .await?;
        Ok(Response::new(Null {}))
    }

    async fn add_child_nexus(
        &self,
        request: Request<AddChildNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        jsonrpc::call::<_, ()>(&self.socket, "add_child_nexus", Some(msg))
            .await?;
        Ok(Response::new(Null {}))
    }

    async fn remove_child_nexus(
        &self,
        request: Request<RemoveChildNexusRequest>,
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        jsonrpc::call::<_, ()>(&self.socket, "remove_child_nexus", Some(msg))
            .await?;
        Ok(Response::new(Null {}))
    }
}
