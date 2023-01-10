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
use futures::{select, FutureExt, StreamExt};
use once_cell::sync::OnceCell;
use std::{borrow::Cow, time::Duration};
use tonic::transport::Server;
use tracing::trace;

static MAYASTOR_GRPC_SERVER: OnceCell<MayastorGrpcServer> = OnceCell::new();

#[derive(Clone)]
pub struct MayastorGrpcServer {
    /// Receive channel for messages and termination
    rcv_chan: async_channel::Receiver<()>,
    /// Termination channel
    fini_chan: async_channel::Sender<()>,
}

impl MayastorGrpcServer {
    /// Get or initialise the grpc server global instance.
    pub fn get_or_init() -> &'static MayastorGrpcServer {
        let (msg_sender, msg_receiver) = async_channel::unbounded::<()>();
        MAYASTOR_GRPC_SERVER.get_or_init(|| MayastorGrpcServer {
            rcv_chan: msg_receiver,
            fini_chan: msg_sender,
        })
    }

    /// Terminate the grpc server.
    pub fn fini(&self) {
        self.fini_chan.close();
    }

    /// Start the grpc server.
    pub async fn run(
        node_name: &str,
        node_nqn: &Option<String>,
        endpoint: std::net::SocketAddr,
        rpc_addr: String,
        api_versions: Vec<ApiVersion>,
    ) -> Result<(), ()> {
        let mut rcv_chan = Self::get_or_init().rcv_chan.clone();

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

        select! {
            result = svc.fuse() => {
                match result {
                    Ok(result) => {
                        trace!(?result);
                        Ok(())
                    }
                    Err(e) => {
                        error!("gRPC server failed with error: {}", e);
                        Err(())
                    }
                }
            },
            _ = rcv_chan.next().fuse() => {
                info!("Shutting down grpc server");
                Ok(())
            }
        }
    }
}
