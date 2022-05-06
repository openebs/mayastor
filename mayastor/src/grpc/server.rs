use crate::grpc::{
    bdev_grpc::BdevSvc,
    json_grpc::JsonRpcSvc,
    mayastor_grpc::MayastorSvc,
    v1::{
        bdev::BdevService,
        host::HostService,
        json::JsonService,
        nexus::NexusService,
        pool::PoolService,
        replica::ReplicaService,
    },
};

use rpc::mayastor::{
    bdev_rpc_server::BdevRpcServer,
    json_rpc_server::JsonRpcServer,
    mayastor_server::MayastorServer as MayastorRpcServer,
    v1,
};

use std::{borrow::Cow, time::Duration};
use tonic::transport::Server;
use tracing::trace;

pub struct MayastorGrpcServer;

impl MayastorGrpcServer {
    pub async fn run(
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
    ) -> Result<(), ()> {
        info!("gRPC server configured at address {}", endpoint);
        let address = Cow::from(rpc_addr);
        let svc = Server::builder()
            .add_service(MayastorRpcServer::new(MayastorSvc::new(
                Duration::from_millis(4),
            )))
            .add_service(BdevRpcServer::new(BdevSvc::new()))
            .add_service(v1::bdev::BdevRpcServer::new(BdevService::new()))
            .add_service(JsonRpcServer::new(JsonRpcSvc::new(address.clone())))
            .add_service(v1::json::JsonRpcServer::new(JsonService::new(
                address.clone(),
            )))
            .add_service(v1::pool::PoolRpcServer::new(PoolService::new()))
            .add_service(v1::replica::ReplicaRpcServer::new(
                ReplicaService::new(),
            ))
            .add_service(v1::host::HostRpcServer::new(HostService::new()))
            .add_service(v1::nexus::NexusRpcServer::new(NexusService::new()))
            .serve(endpoint);

        match svc.await {
            Ok(result) => {
                trace!(?result);
                Ok(())
            }
            Err(e) => {
                error!("gRPC server failed with error: {}", e);
                Err(())
            }
        }
    }
}
