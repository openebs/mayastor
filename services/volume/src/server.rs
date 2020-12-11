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

                let service: &VolumeSvc = args.context.get_state();
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
impl_service_handler!(GetNexuses, get_nexuses);
impl_service_handler!(CreateNexus, create_nexus);
impl_service_handler!(DestroyNexus, destroy_nexus);
impl_service_handler!(ShareNexus, share_nexus);
impl_service_handler!(UnshareNexus, unshare_nexus);
impl_service_handler!(AddNexusChild, add_nexus_child);
impl_service_handler!(RemoveNexusChild, remove_nexus_child);
// volumes
impl_service_handler!(GetVolumes, get_volumes);
impl_service_handler!(CreateVolume, create_volume);
impl_service_handler!(DestroyVolume, destroy_volume);

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
    Service::builder(cli_args.nats, ChannelVs::Volume)
        .connect()
        .await
        .with_shared_state(VolumeSvc::new(cli_args.period.into()))
        .with_default_liveness()
        .with_subscription(ServiceHandler::<GetVolumes>::default())
        .with_subscription(ServiceHandler::<CreateVolume>::default())
        .with_subscription(ServiceHandler::<DestroyVolume>::default())
        .with_channel(ChannelVs::Nexus)
        .with_subscription(ServiceHandler::<GetNexuses>::default())
        .with_subscription(ServiceHandler::<CreateNexus>::default())
        .with_subscription(ServiceHandler::<DestroyNexus>::default())
        .with_subscription(ServiceHandler::<ShareNexus>::default())
        .with_subscription(ServiceHandler::<UnshareNexus>::default())
        .with_subscription(ServiceHandler::<AddNexusChild>::default())
        .with_subscription(ServiceHandler::<RemoveNexusChild>::default())
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
        Liveness {}.request_on(ChannelVs::Volume).await.unwrap();
    }
    // to avoid waiting for timeouts
    async fn orderly_start(test: &ComposeTest) {
        test.start_containers(vec!["nats", "node", "pool", "volume"])
            .await
            .unwrap();

        test.connect_to_bus("nats").await;
        wait_for_services().await;

        test.start("mayastor").await.unwrap();
        test.start("mayastor2").await.unwrap();

        let mut hdl = test.grpc_handle("mayastor").await.unwrap();
        hdl.mayastor.list_nexus(Null {}).await.unwrap();
        let mut hdl = test.grpc_handle("mayastor2").await.unwrap();
        hdl.mayastor.list_nexus(Null {}).await.unwrap();
    }

    #[tokio::test]
    async fn volume() {
        let mayastor = "volume-test-name";
        let mayastor2 = "volume-test-name-replica";
        let test = Builder::new()
            .name("volume")
            .add_container_bin("nats", Binary::from_nix("nats-server"))
            .add_container_bin("node", Binary::from_dbg("node").with_nats("-n"))
            .add_container_bin("pool", Binary::from_dbg("pool").with_nats("-n"))
            .add_container_bin(
                "volume",
                Binary::from_dbg("volume").with_nats("-n"),
            )
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor])
                    .with_args(vec!["-g", "10.1.0.6:10124"]),
            )
            .add_container_bin(
                "mayastor2",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor2])
                    .with_args(vec!["-g", "10.1.0.7:10124"]),
            )
            .with_default_tracing()
            .autorun(false)
            .build()
            .await
            .unwrap();

        orderly_start(&test).await;
        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);

        prepare_pools(mayastor, mayastor2).await;
        test_nexus(mayastor, mayastor2).await;
        test_volume().await;

        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    }

    async fn prepare_pools(mayastor: &str, mayastor2: &str) {
        CreatePool {
            node: mayastor.to_string(),
            name: "pooloop".to_string(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        CreatePool {
            node: mayastor2.to_string(),
            name: "pooloop".to_string(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        let pools = GetPools::default().request().await.unwrap();
        tracing::info!("Pools: {:?}", pools);
    }

    async fn test_nexus(mayastor: &str, mayastor2: &str) {
        let replica = CreateReplica {
            node: mayastor2.into(),
            uuid: "replica".into(),
            pool: "pooloop".into(),
            size: 12582912, /* actual size will be a multiple of 4MB so just
                             * create it like so */
            thin: true,
            share: Protocol::Nvmf,
        }
        .request()
        .await
        .unwrap();

        let local = "malloc:///local?size_mb=12".into();

        let nexus = CreateNexus {
            node: mayastor.into(),
            uuid: "f086f12c-1728-449e-be32-9415051090d6".into(),
            size: 5242880,
            children: vec![replica.uri, local],
        }
        .request()
        .await
        .unwrap();

        let nexuses = GetNexuses::default().request().await.unwrap().0;
        tracing::info!("Nexuses: {:?}", nexuses);
        assert_eq!(Some(&nexus), nexuses.first());

        ShareNexus {
            node: mayastor.into(),
            uuid: "f086f12c-1728-449e-be32-9415051090d6".into(),
            key: None,
            protocol: Protocol::Nvmf,
        }
        .request()
        .await
        .unwrap();

        DestroyNexus {
            node: mayastor.into(),
            uuid: "f086f12c-1728-449e-be32-9415051090d6".to_string(),
        }
        .request()
        .await
        .unwrap();

        DestroyReplica {
            node: replica.node.to_string(),
            pool: replica.pool.to_string(),
            uuid: replica.uuid.to_string(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    }

    async fn test_volume() {
        let volume = CreateVolume {
            uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".to_string(),
            size: 5242880,
            nexuses: 1,
            replicas: 2,
            allowed_nodes: vec![],
            preferred_nodes: vec![],
            preferred_nexus_nodes: vec![],
        };

        let volume = volume.request().await.unwrap();
        let volumes = GetVolumes::default().request().await.unwrap().0;
        tracing::info!("Volumes: {:?}", volumes);

        assert_eq!(Some(&volume), volumes.first());

        DestroyVolume {
            uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".to_string(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
        assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
    }
}
