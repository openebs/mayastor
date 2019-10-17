//! Implementation of gRPC methods from mayastor gRPC service.

use tonic::{Code, Request, Response, Status};

use rpc::jsonrpc as jsondata;

use crate::rpc::{
    mayastor::{ShareProtocol, *},
    service,
};

/// mayastorService handles non CSI rpc calls
#[derive(Clone, Debug)]
pub struct MayastorService {
    pub socket: String,
}
impl MayastorService {}
#[tonic::async_trait]
impl service::server::Mayastor for MayastorService {
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
    ) -> Result<Response<Null>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        let uuid = msg.uuid;
        let pool = msg.pool;
        let share = match ShareProtocol::from_i32(msg.share) {
            Some(ShareProtocol::None) => jsondata::ShareProtocol::None,
            Some(ShareProtocol::Nvmf) => jsondata::ShareProtocol::Nvmf,
            Some(ShareProtocol::Iscsi) => jsondata::ShareProtocol::Iscsi,
            None => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "Invalid value of share protocol".to_owned(),
                ));
            }
        };

        debug!("Creating replica {} on {} ...", uuid, pool);

        let args = Some(jsondata::CreateReplicaArgs {
            uuid: uuid.clone(),
            pool: pool.clone(),
            thin_provision: msg.thin,
            size: msg.size,
            share,
        });

        match jsonrpc::call::<_, ()>(&self.socket, "create_replica", args).await
        {
            Ok(_) => Ok(Response::new(Null {})),
            Err(err) => {
                error!(
                    "Failed to create replica {} on {}: {}",
                    uuid, pool, err
                );
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

        let uuid = msg.uuid;
        debug!("Destroying replica {} ...", uuid);

        let args = Some(jsondata::DestroyReplicaArgs {
            uuid: uuid.clone(),
        });

        match jsonrpc::call::<_, ()>(&self.socket, "destroy_replica", args)
            .await
        {
            Ok(_) => {
                info!("Destroyed replica {}", uuid);
                Ok(Response::new(Null {}))
            }
            Err(err) => {
                error!("Failed to destroy replica {}: {}", uuid, err);
                Err(err.into())
            }
        }
    }

    /// List replicas
    async fn list_replicas(
        &self,
        request: Request<Null>,
    ) -> Result<Response<ListReplicasReply>, Status> {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        match jsonrpc::call::<(), Vec<jsondata::Replica>>(
            &self.socket,
            "list_replicas",
            None,
        )
        .await
        {
            Ok(replicas) => {
                debug!("Got list of {} replicas", replicas.len());
                let resp = Response::new(ListReplicasReply {
                    replicas: replicas
                        .iter()
                        .map(|r| Replica {
                            uuid: r.uuid.clone(),
                            pool: r.pool.clone(),
                            thin: r.thin_provision,
                            size: r.size,
                            share: match r.share {
                                jsondata::ShareProtocol::None => {
                                    ShareProtocol::None
                                }
                                jsondata::ShareProtocol::Nvmf => {
                                    ShareProtocol::Nvmf
                                }
                                jsondata::ShareProtocol::Iscsi => {
                                    ShareProtocol::Iscsi
                                }
                            } as i32,
                        })
                        .collect(),
                });
                trace!("{:?}", resp);
                Ok(resp)
            }
            Err(err) => {
                error!("Getting replicas failed: {}", err);
                Err(err.into())
            }
        }
    }

    /// Return replica stats
    async fn stat_replicas(
        &self,
        request: Request<Null>,
    ) -> Result<Response<StatReplicasReply>, Status> {
        let msg = request.into_inner();
        let socket = &self.socket;

        trace!("{:?}", msg);

        match jsonrpc::call::<(), Vec<jsondata::Stats>>(
            socket,
            "stat_replicas",
            None,
        )
        .await
        {
            Ok(stats) => {
                let resp = Response::new(StatReplicasReply {
                    replicas: stats
                        .iter()
                        .map(|st| ReplicaStats {
                            uuid: st.uuid.clone(),
                            pool: st.pool.clone(),
                            stats: Some(Stats {
                                num_read_ops: st.num_read_ops,
                                num_write_ops: st.num_write_ops,
                                bytes_read: st.bytes_read,
                                bytes_written: st.bytes_written,
                            }),
                        })
                        .collect(),
                });
                trace!("{:?}", resp);
                Ok(resp)
            }
            Err(err) => {
                error!("Getting replicas failed: {}", err);
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
}
