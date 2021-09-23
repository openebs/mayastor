use crate::grpc::{
    bdev::v1::BdevSvc as BdevSvc_v1, bdev_grpc::BdevSvc,
    json::v1::JsonRpcSvc as JsonRpcSvc_v1, json_grpc::JsonRpcSvc,
    mayastor_grpc::MayastorSvc,
};
use rpc::mayastor::{
    bdev_rpc_server::BdevRpcServer,
    json_rpc_server::JsonRpcServer,
    mayastor_server::MayastorServer as MayastorRpcServer,
};

use rpc::mayastorv1::{
    bdev_rpc_server::BdevRpcServer as BdevRpcServer_v1,
    json_rpc_server::JsonRpcServer as JsonRpcServer_v1,
};
use std::time::Duration;
use tonic::transport::Server;

pub struct MayastorGrpcServer;

impl MayastorGrpcServer {
    pub async fn run(
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
    ) -> Result<(), ()> {
        info!("gRPC server configured at address {}", endpoint);
        let address = rpc_addr.clone();
        let svc = Server::builder()
            .add_service(MayastorRpcServer::new(MayastorSvc::new(
                Duration::from_millis(4),
            )))
            .add_service(BdevRpcServer::new(BdevSvc::new()))
            .add_service(BdevRpcServer_v1::new(BdevSvc_v1::new()))
            .add_service(JsonRpcServer::new(JsonRpcSvc { rpc_addr }))
            .add_service(JsonRpcServer_v1::new(JsonRpcSvc_v1 {
                rpc_addr: address,
            }))
            .serve(endpoint);

        match svc.await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("gRPC server failed with error: {}", e);
                Err(())
            }
        }
    }
}
