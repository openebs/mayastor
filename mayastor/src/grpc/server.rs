use tonic::transport::Server;

use crate::grpc::{bdev_grpc::BdevSvc, mayastor_grpc::MayastorSvc};
use rpc::mayastor::{
    bdev_rpc_server::BdevRpcServer,
    mayastor_server::MayastorServer as MayastorRpcServer,
};

pub struct MayastorGrpcServer {}

impl MayastorGrpcServer {
    pub async fn run(endpoint: std::net::SocketAddr) -> Result<(), ()> {
        info!("gRPC server configured at address {}", endpoint);
        let svc = Server::builder()
            .add_service(MayastorRpcServer::new(MayastorSvc {}))
            .add_service(BdevRpcServer::new(BdevSvc {}))
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
