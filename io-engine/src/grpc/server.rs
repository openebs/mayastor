use super::{
    v0::{
        bdev_grpc::BdevSvc,
        json_grpc::JsonRpcSvc,
        mayastor_grpc::MayastorSvc,
    },
    v1::{
        bdev::BdevService,
        host::HostService,
        json::JsonService,
        nexus::NexusService,
        pool::PoolService,
        replica::ReplicaService,
    },
};

use mayastor_api::{
    v0::{
        bdev_rpc_server::BdevRpcServer,
        json_rpc_server::JsonRpcServer,
        mayastor_server::MayastorServer as MayastorRpcServer,
    },
    v1,
};

use crate::subsys::registration::registration_grpc::ApiVersion;
use std::{borrow::Cow, time::Duration};
use tonic::transport::Server;
use tracing::trace;

pub struct MayastorGrpcServer;

impl MayastorGrpcServer {
    pub async fn run(
        node_name: &str,
        node_nqn: &Option<String>,
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
        api_versions: Vec<ApiVersion>,
    ) -> Result<(), ()> {
        let address = Cow::from(rpc_addr);

        let enable_v0 = api_versions.contains(&ApiVersion::V0).then(|| true);
        let enable_v1 = api_versions.contains(&ApiVersion::V1).then(|| true);
        info!(
            "{:?} gRPC server configured at address {}",
            api_versions, endpoint
        );
        let svc = Server::builder()
            .add_optional_service(
                enable_v1
                    .map(|_| v1::bdev::BdevRpcServer::new(BdevService::new())),
            )
            .add_optional_service(enable_v1.map(|_| {
                v1::json::JsonRpcServer::new(JsonService::new(address.clone()))
            }))
            .add_optional_service(
                enable_v1
                    .map(|_| v1::pool::PoolRpcServer::new(PoolService::new())),
            )
            .add_optional_service(enable_v1.map(|_| {
                v1::replica::ReplicaRpcServer::new(ReplicaService::new())
            }))
            .add_optional_service(enable_v1.map(|_| {
                v1::host::HostRpcServer::new(HostService::new(
                    node_name,
                    node_nqn,
                    endpoint,
                    api_versions,
                ))
            }))
            .add_optional_service(
                enable_v1.map(|_| {
                    v1::nexus::NexusRpcServer::new(NexusService::new())
                }),
            )
            .add_optional_service(enable_v0.map(|_| {
                MayastorRpcServer::new(MayastorSvc::new(Duration::from_millis(
                    4,
                )))
            }))
            .add_optional_service(
                enable_v0.map(|_| {
                    JsonRpcServer::new(JsonRpcSvc::new(address.clone()))
                }),
            )
            .add_optional_service(
                enable_v0.map(|_| BdevRpcServer::new(BdevSvc::new())),
            )
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
