pub mod service;

use async_trait::async_trait;
use common::*;
use mbus_api::{v0::*, *};
use service::*;
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    nats: String,

    /// The period at which the registry updates its cache of all
    /// resources from all nodes
    #[structopt(long, short, default_value = "20s")]
    period: humantime::Duration,
}

/// Needed so we can implement the ServiceSubscriber trait for
/// the message types external to the crate
#[derive(Clone, Default)]
struct ServiceHandler<T> {
    data: PhantomData<T>,
}

macro_rules! impl_service_handler {
    // RequestType is the message bus request type
    // ServiceFnName is the name of the service function to route the request
    // into
    ($RequestType:ident, $ServiceFnName:ident) => {
        #[async_trait]
        impl ServiceSubscriber for ServiceHandler<$RequestType> {
            async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
                let request: ReceivedMessage<$RequestType> =
                    args.request.try_into()?;

                let service: &PoolSvc = args.context.get_state()?;
                let reply = service
                    .$ServiceFnName(&request.inner())
                    .await
                    .map_err(|error| Error::ServiceError {
                        message: error.full_string(),
                    })?;
                request.reply(reply).await
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$RequestType::default().id()]
            }
        }
    };
}

// todo:
// a service handler can actually specify a vector of message filters so could
// indeed do the filtering at our service specific code and have a single
// entrypoint here nexus
impl_service_handler!(GetPools, get_pools);
impl_service_handler!(GetReplicas, get_replicas);
impl_service_handler!(CreatePool, create_pool);
impl_service_handler!(DestroyPool, destroy_pool);
impl_service_handler!(CreateReplica, create_replica);
impl_service_handler!(DestroyReplica, destroy_replica);
impl_service_handler!(ShareReplica, share_replica);
impl_service_handler!(UnshareReplica, unshare_replica);

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[tokio::main]
async fn main() {
    init_tracing();

    let cli_args = CliArgs::from_args();
    info!("Using options: {:?}", &cli_args);

    server(cli_args).await;
}

async fn server(cli_args: CliArgs) {
    Service::builder(cli_args.nats, ChannelVs::Pool)
        .connect()
        .await
        .with_shared_state(PoolSvc::new(cli_args.period.into()))
        .with_default_liveness()
        .with_subscription(ServiceHandler::<GetPools>::default())
        .with_subscription(ServiceHandler::<GetReplicas>::default())
        .with_subscription(ServiceHandler::<CreatePool>::default())
        .with_subscription(ServiceHandler::<DestroyPool>::default())
        .with_subscription(ServiceHandler::<CreateReplica>::default())
        .with_subscription(ServiceHandler::<DestroyReplica>::default())
        .with_subscription(ServiceHandler::<ShareReplica>::default())
        .with_subscription(ServiceHandler::<UnshareReplica>::default())
        .run()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use composer::*;
    use rpc::mayastor::Null;

    async fn wait_for_services() {
        let _ = GetNodes {}.request().await.unwrap();
        Liveness {}.request_on(ChannelVs::Pool).await.unwrap();
    }
    // to avoid waiting for timeouts
    async fn orderly_start(test: &ComposeTest) {
        test.start_containers(vec!["nats", "node", "pool"])
            .await
            .unwrap();

        test.connect_to_bus("nats").await;
        wait_for_services().await;

        test.start("mayastor").await.unwrap();

        let mut hdl = test.grpc_handle("mayastor").await.unwrap();
        hdl.mayastor.list_nexus(Null {}).await.unwrap();
    }

    #[tokio::test]
    async fn pool() {
        let mayastor = "pool-test-name";
        let test = Builder::new()
            .name("pool")
            .add_container_bin(
                "nats",
                Binary::from_nix("nats-server").with_arg("-DV"),
            )
            .add_container_bin("node", Binary::from_dbg("node").with_nats("-n"))
            .add_container_bin("pool", Binary::from_dbg("pool").with_nats("-n"))
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor])
                    .with_args(vec!["-g", "10.1.0.5:10124"]),
            )
            .with_default_tracing()
            .autorun(false)
            .build()
            .await
            .unwrap();

        orderly_start(&test).await;

        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);

        CreatePool {
            node: mayastor.into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        let pools = GetPools::default().request().await.unwrap();
        tracing::info!("Pools: {:?}", pools);

        let replica = CreateReplica {
            node: mayastor.into(),
            uuid: "replica1".into(),
            pool: "pooloop".into(),
            size: 12582912, /* actual size will be a multiple of 4MB so just
                             * create it like so */
            thin: true,
            share: Protocol::Off,
        }
        .request()
        .await
        .unwrap();

        let replicas = GetReplicas::default().request().await.unwrap();
        tracing::info!("Replicas: {:?}", replicas);

        assert_eq!(
            replica,
            Replica {
                node: mayastor.into(),
                uuid: "replica1".into(),
                pool: "pooloop".into(),
                thin: false,
                size: 12582912,
                share: Protocol::Off,
                uri: "bdev:///replica1".into()
            }
        );

        let uri = ShareReplica {
            node: mayastor.into(),
            uuid: "replica1".into(),
            pool: "pooloop".into(),
            protocol: Protocol::Nvmf,
        }
        .request()
        .await
        .unwrap();

        let mut replica_updated = replica;
        replica_updated.uri = uri;
        replica_updated.share = Protocol::Nvmf;
        let replica = GetReplicas::default().request().await.unwrap();
        let replica = replica.0.first().unwrap();
        assert_eq!(replica, &replica_updated);

        DestroyReplica {
            node: mayastor.into(),
            uuid: "replica1".into(),
            pool: "pooloop".into(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetReplicas::default().request().await.unwrap().0.is_empty());

        DestroyPool {
            node: mayastor.into(),
            id: "pooloop".into(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetPools::default().request().await.unwrap().0.is_empty());
    }
}
