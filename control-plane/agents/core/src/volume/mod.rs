pub(crate) mod registry;
mod service;

use std::{convert::TryInto, marker::PhantomData};

use super::{core::registry::Registry, handler, impl_request_handler};
use async_trait::async_trait;
use common::errors::SvcError;
use mbus_api::{v0::*, *};

pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    builder
        .with_channel(ChannelVs::Volume)
        .with_default_liveness()
        .with_shared_state(service::Service::new(registry))
        .with_subscription(handler!(GetVolumes))
        .with_subscription(handler!(CreateVolume))
        .with_subscription(handler!(DestroyVolume))
        .with_channel(ChannelVs::Nexus)
        .with_subscription(handler!(GetNexuses))
        .with_subscription(handler!(CreateNexus))
        .with_subscription(handler!(DestroyNexus))
        .with_subscription(handler!(ShareNexus))
        .with_subscription(handler!(UnshareNexus))
        .with_subscription(handler!(AddNexusChild))
        .with_subscription(handler!(RemoveNexusChild))
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
        test.start_containers(vec!["nats", "core"]).await.unwrap();

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
            .add_container_bin("core", Binary::from_dbg("core").with_nats("-n"))
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor])
                    .with_args(vec!["-g", "10.1.0.4:10124"]),
            )
            .add_container_bin(
                "mayastor2",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor2])
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

        prepare_pools(mayastor, mayastor2).await;
        test_nexus(mayastor, mayastor2).await;
        test_volume().await;

        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    }

    async fn prepare_pools(mayastor: &str, mayastor2: &str) {
        CreatePool {
            node: mayastor.into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk0?size_mb=100".into()],
        }
        .request()
        .await
        .unwrap();

        CreatePool {
            node: mayastor2.into(),
            id: "pooloop".into(),
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
            children: vec![replica.uri.into(), local],
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
            uuid: "f086f12c-1728-449e-be32-9415051090d6".into(),
        }
        .request()
        .await
        .unwrap();

        DestroyReplica {
            node: replica.node,
            pool: replica.pool,
            uuid: replica.uuid,
        }
        .request()
        .await
        .unwrap();

        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
    }

    async fn test_volume() {
        let volume = CreateVolume {
            uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
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
            uuid: "359b7e1a-b724-443b-98b4-e6d97fabbb40".into(),
        }
        .request()
        .await
        .unwrap();

        assert!(GetVolumes::default().request().await.unwrap().0.is_empty());
        assert!(GetNexuses::default().request().await.unwrap().0.is_empty());
        assert!(GetReplicas::default().request().await.unwrap().0.is_empty());
    }
}
