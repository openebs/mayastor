mod registry;
pub mod service;

use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use async_trait::async_trait;
use common::{errors::SvcError, Service};
use mbus_api::{
    v0::{
        ChannelVs,
        CreatePool,
        CreateReplica,
        DestroyPool,
        DestroyReplica,
        GetPools,
        GetReplicas,
        ShareReplica,
        UnshareReplica,
    },
    Message,
    MessageId,
    ReceivedMessage,
};

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Pool)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetPools))
        .with_subscription(handler!(CreatePool))
        .with_subscription(handler!(DestroyPool))
        .with_subscription(handler!(GetReplicas))
        .with_subscription(handler!(CreateReplica))
        .with_subscription(handler!(DestroyReplica))
        .with_subscription(handler!(ShareReplica))
        .with_subscription(handler!(UnshareReplica))
}

#[cfg(test)]
mod tests {
    use super::*;
    use composer::*;
    use mbus_api::v0::{GetNodes, Liveness, Protocol, Replica};
    use rpc::mayastor::Null;

    async fn wait_for_services() {
        let _ = GetNodes {}.request().await.unwrap();
        Liveness {}.request_on(ChannelVs::Pool).await.unwrap();
    }
    // to avoid waiting for timeouts
    async fn orderly_start(test: &ComposeTest) {
        test.start_containers(vec!["nats", "core"]).await.unwrap();

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
            .add_container_bin("core", Binary::from_dbg("core").with_nats("-n"))
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor])
                    .with_args(vec!["-g", "10.1.0.4:10124"]),
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
