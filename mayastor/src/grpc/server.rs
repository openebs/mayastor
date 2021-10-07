use crate::grpc::{
    bdev_grpc::BdevSvc,
    json_grpc::JsonRpcSvc,
    mayastor_grpc::MayastorSvc,
};

use crate::grpc::{
    bdev::v1::BdevService,
    json::v1::JsonService,
};

use rpc::mayastor::{
    bdev_rpc_server::BdevRpcServer,
    json_rpc_server::JsonRpcServer,
    mayastor_server::MayastorServer as MayastorRpcServer,
    v1,
};

use std::time::Duration;
use tonic::transport::Server;
use tracing::trace;
use std::borrow::Cow;

pub struct MayastorGrpcServer;

impl MayastorGrpcServer {
    pub async fn run(
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
    ) -> Result<(), ()> {
        info!("gRPC server configured at address {}", endpoint);
        let  address = Cow::from(rpc_addr);
        let svc = Server::builder()
            .add_service(MayastorRpcServer::new(MayastorSvc::new(
                Duration::from_millis(4),
            )))
            .add_service(BdevRpcServer::new(BdevSvc::new()))
            .add_service(v1::BdevRpcServer::new(BdevService::new()))
            .add_service(JsonRpcServer::new(JsonRpcSvc::new(address.clone())))
            .add_service(v1::JsonRpcServer::new(JsonService::new(address.clone())))
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
