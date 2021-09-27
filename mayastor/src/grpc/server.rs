use crate::grpc::mayastor_grpc::MayastorSvc;

use crate::grpc::{bdev::v1::BdevService, json::v1::JsonService};

use rpc::mayastor::{mayastor_server::MayastorServer as MayastorRpcServer, v1};

use std::time::Duration;
use tonic::transport::Server;
use tracing::trace;
pub struct MayastorGrpcServer;

impl MayastorGrpcServer {
    pub async fn run(
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
    ) -> Result<(), ()> {
        info!("gRPC server configured at address {}", endpoint);
        let rpc_addr = rpc_addr.clone();
        let svc = Server::builder()
            .add_service(MayastorRpcServer::new(MayastorSvc::new(
                Duration::from_millis(4),
            )))
            .add_service(v1::BdevRpcServer::new(BdevService::new()))
            .add_service(rpc::mayastor::v1::BdevRpcServer::new(
                BdevService::new(),
            ))
            .add_service(v1::JsonRpcServer::new(JsonService::new(rpc_addr)))
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
