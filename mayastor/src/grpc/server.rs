use tonic::transport::Server;

use crate::grpc::{
    bdev_grpc::BdevSvc,
    json_grpc::JsonRpcSvc,
    mayastor_grpc::MayastorSvc,
};
use rpc::mayastor::{
    bdev_rpc_server::BdevRpcServer,
    json_rpc_server::JsonRpcServer,
    mayastor_server::MayastorServer as MayastorRpcServer,
};

pub struct MayastorGrpcServer;

impl MayastorGrpcServer {
    pub async fn run(
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
    ) -> Result<(), ()> {
        info!("gRPC server configured at address {}", endpoint);
        let svc = Server::builder()
            .add_service(MayastorRpcServer::new(MayastorSvc))
            .add_service(BdevRpcServer::new(BdevSvc::new()))
            .add_service(JsonRpcServer::new(JsonRpcSvc {
                rpc_addr,
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
