//! Implementation of gRPC methods from mayastor gRPC service.

use crate::rpc::{mayastor::*, service};

use enclose::enclose;
use futures::future;

use futures::future::Future;
use jsonrpc;
use rpc::{jsonrpc as jsondata, mayastor::ListNexusReply};
use std::{boxed::Box, vec::Vec};
use tower_grpc::{Code, Request, Response, Status};
/// mayastorService handles non CSI rpc calls
#[derive(Clone, Debug)]
pub struct MayastorService {
    pub socket: String,
}

impl service::server::Mayastor for MayastorService {
    // Definition of exact return values from method handlers.
    // We take the perf penalty of boxing the values and using virtual dispatch
    // table on returned object to overcome otherwise different types of return
    // values (future::ok vs jsonrpc::call).
    type CreatePoolFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type DestroyPoolFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type ListPoolsFuture = Box<
        dyn future::Future<Item = Response<ListPoolsReply>, Error = Status>
            + Send,
    >;
    type CreateReplicaFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type DestroyReplicaFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type ListReplicasFuture = Box<
        dyn future::Future<Item = Response<ListReplicasReply>, Error = Status>
            + Send,
    >;
    type StatReplicasFuture = Box<
        dyn future::Future<Item = Response<StatReplicasReply>, Error = Status>
            + Send,
    >;
    type CreateNexusFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type DestroyNexusFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type ListNexusFuture = Box<
        dyn future::Future<Item = Response<ListNexusReply>, Error = Status>
            + Send,
    >;
    type PublishNexusFuture = Box<
        dyn future::Future<Item = Response<PublishNexusReply>, Error = Status>
            + Send,
    >;
    type UnpublishNexusFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;
    type ChildOperationFuture =
        Box<dyn future::Future<Item = Response<Null>, Error = Status> + Send>;

    /// Create storage pool (or import it if it already exists on the
    /// specified disk).
    fn create_pool(
        &mut self,
        request: Request<CreatePoolRequest>,
    ) -> Self::CreatePoolFuture {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        if msg.disks.is_empty() {
            return Box::new(future::err(Status::new(
                Code::InvalidArgument,
                "Missing device".to_string(),
            )));
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

        let f =
            jsonrpc::call::<_, ()>(&self.socket, "create_or_import_pool", args)
                .map(enclose! { (pool_name) move |_| {
                    info!("Created or imported pool {}", pool_name);
                    Response::new(Null {})
                }})
                .map_err(enclose! { (pool_name) move |err| {
                    error!("Failed to create pool {}: {}", pool_name, err);
                    err.into_status()
                }});

        Box::new(f)
    }

    /// Destroy pool -> destroy lvol store and delete underlying base bdev.
    fn destroy_pool(
        &mut self,
        request: Request<DestroyPoolRequest>,
    ) -> Self::DestroyPoolFuture {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        let args = Some(jsondata::DestroyPoolArgs {
            name: msg.name.clone(),
        });

        // make a copy of vars used in the closures below
        let socket = self.socket.clone();
        let pool_name = msg.name;

        debug!("Destroying pool {} ...", pool_name);

        let f = jsonrpc::call::<_, ()>(&socket, "destroy_pool", args)
            .map(enclose! { (pool_name) move |_| {
                info!("Destroyed pool {}", pool_name);
                Response::new(Null {})
            }})
            .map_err(enclose! { (pool_name) move |err| {
                error!("Failed to destroy pool {}: {}", pool_name, err);
                err.into_status()
            }});

        Box::new(f)
    }

    /// Get list of lvol stores.
    ///
    /// TODO: There is a state field which is always set to "online" state.
    /// Figure out how to set it properly.
    fn list_pools(&mut self, request: Request<Null>) -> Self::ListPoolsFuture {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        let f = jsonrpc::call::<(), Vec<jsondata::Pool>>(
            &self.socket,
            "list_pools",
            None,
        )
        .map(move |pools| {
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
            resp
        })
        .map_err(|err| {
            error!("Getting lvol stores failed: {}", err);
            err.into_status()
        });

        Box::new(f)
    }

    /// Create replica
    fn create_replica(
        &mut self,
        request: Request<CreateReplicaRequest>,
    ) -> Self::CreateReplicaFuture {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        let uuid = msg.uuid;
        let pool = msg.pool;
        let share = match ShareProtocol::from_i32(msg.share) {
            Some(ShareProtocol::None) => jsondata::ShareProtocol::None,
            Some(ShareProtocol::Nvmf) => jsondata::ShareProtocol::Nvmf,
            Some(ShareProtocol::Iscsi) => jsondata::ShareProtocol::Iscsi,
            None => {
                return Box::new(future::err(Status::new(
                    Code::InvalidArgument,
                    "Invalid value of share protocol".to_owned(),
                )))
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

        let f = jsonrpc::call::<_, ()>(&self.socket, "create_replica", args)
            .map(enclose! { (uuid, pool) move |_| {
                info!("Created replica {} on pool {}", uuid, pool);
                Response::new(Null {})
            }})
            .map_err(enclose! { (uuid, pool) move |err| {
                error!("Failed to create replica {} on {}: {}",
                       uuid, pool, err);
                err.into_status()
            }});

        Box::new(f)
    }

    /// Destroy replica
    fn destroy_replica(
        &mut self,
        request: Request<DestroyReplicaRequest>,
    ) -> Self::DestroyReplicaFuture {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        let uuid = msg.uuid.clone();
        debug!("Destroying replica {} ...", uuid);

        let args = Some(jsondata::DestroyReplicaArgs {
            uuid: uuid.clone(),
        });

        let f = jsonrpc::call::<_, ()>(&self.socket, "destroy_replica", args)
            .map(enclose! { (uuid) move |_| {
                info!("Destroyed replica {}", uuid);
                Response::new(Null {})
            }})
            .map_err(enclose! { (uuid) move |err| {
                error!("Failed to destroy replica {}: {}", uuid, err);
                err.into_status()
            }});

        Box::new(f)
    }

    /// List replicas
    fn list_replicas(
        &mut self,
        request: Request<Null>,
    ) -> Self::ListReplicasFuture {
        let msg = request.into_inner();

        trace!("{:?}", msg);

        let f = jsonrpc::call::<(), Vec<jsondata::Replica>>(
            &self.socket,
            "list_replicas",
            None,
        )
        .map(move |replicas| {
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
            resp
        })
        .map_err(|err| {
            error!("Getting replicas failed: {}", err);
            err.into_status()
        });

        Box::new(f)
    }

    /// Return replica stats
    fn stat_replicas(
        &mut self,
        request: Request<Null>,
    ) -> Self::StatReplicasFuture {
        let msg = request.into_inner();
        let socket = &self.socket;

        trace!("{:?}", msg);

        let f = jsonrpc::call::<(), Vec<jsondata::Stats>>(
            socket,
            "stat_replicas",
            None,
        )
        .map(move |stats| {
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
            resp
        })
        .map_err(|err| {
            error!("Getting replicas failed: {}", err);
            err.into_status()
        });

        Box::new(f)
    }

    fn create_nexus(
        &mut self,
        request: Request<CreateNexusRequest>,
    ) -> Self::CreateNexusFuture {
        let msg = request.into_inner();
        trace!("{:?}", msg);

        Box::new(
            jsonrpc::call::<_, ()>(&self.socket, "create_nexus", Some(msg))
                .map_err(|e| e.into_status())
                .map(|_| Response::new(Null {})),
        )
    }

    fn destroy_nexus(
        &mut self,
        request: Request<DestroyNexusRequest>,
    ) -> Self::DestroyNexusFuture {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        Box::new(
            jsonrpc::call::<_, ()>(&self.socket, "destroy_nexus", Some(msg))
                .map_err(|e| e.into_status())
                .map(|_| Response::new(Null {})),
        )
    }

    fn list_nexus(&mut self, _request: Request<Null>) -> Self::ListNexusFuture {
        Box::new(
            jsonrpc::call::<(), ListNexusReply>(
                &self.socket,
                "list_nexus",
                None,
            )
            .map_err(|e| e.into_status())
            .map(Response::new),
        )
    }

    fn publish_nexus(
        &mut self,
        request: Request<PublishNexusRequest>,
    ) -> Self::PublishNexusFuture {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        Box::new(
            jsonrpc::call(&self.socket, "publish_nexus", Some(msg))
                .map_err(|e| e.into_status())
                .map(Response::new),
        )
    }

    fn unpublish_nexus(
        &mut self,
        request: Request<UnpublishNexusRequest>,
    ) -> Self::UnpublishNexusFuture {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        Box::new(
            jsonrpc::call::<_, ()>(&self.socket, "unpublish_nexus", Some(msg))
                .map_err(|e| e.into_status())
                .map(|_| Response::new(Null {})),
        )
    }

    fn child_operation(
        &mut self,
        request: Request<ChildNexusRequest>,
    ) -> Self::ChildOperationFuture {
        let msg = request.into_inner();
        Box::new(
            jsonrpc::call::<_, ()>(&self.socket, "offline_child", Some(msg))
                .map_err(|e| e.into_status())
                .map(|_| Response::new(Null {})),
        )
    }
}
