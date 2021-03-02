pub(super) mod service;
/// node watchdog to keep track of a node's liveness
pub(crate) mod watchdog;

use super::{
    core::registry,
    handler,
    handler_publish,
    impl_publish_handler,
    impl_request_handler,
    CliArgs,
};
use common::{errors::SvcError, Service};
use mbus_api::{v0::*, *};

use async_trait::async_trait;
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.get_shared_state::<registry::Registry>().clone();
    let deadline = CliArgs::from_args().deadline.into();
    builder
        .with_shared_state(service::Service::new(registry, deadline))
        .with_channel(ChannelVs::Registry)
        .with_subscription(handler_publish!(Register))
        .with_subscription(handler_publish!(Deregister))
        .with_channel(ChannelVs::Node)
        .with_subscription(handler!(GetNodes))
        .with_subscription(handler!(GetBlockDevices))
        .with_default_liveness()
}

#[cfg(test)]
mod tests {
    use super::*;
    use composer::*;
    use rpc::mayastor::Null;

    async fn bus_init() -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            mbus_api::message_bus_init("10.1.0.2".into()).await
        })
        .await?;
        Ok(())
    }
    async fn wait_for_node() -> Result<(), Box<dyn std::error::Error>> {
        let _ = GetNodes {}.request().await?;
        Ok(())
    }
    fn init_tracing() {
        if let Ok(filter) =
            tracing_subscriber::EnvFilter::try_from_default_env()
        {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        } else {
            tracing_subscriber::fmt().with_env_filter("info").init();
        }
    }
    // to avoid waiting for timeouts
    async fn orderly_start(
        test: &ComposeTest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        test.start_containers(vec!["nats", "core"]).await?;

        bus_init().await?;
        wait_for_node().await?;

        test.start("mayastor").await?;

        let mut hdl = test.grpc_handle("mayastor").await?;
        hdl.mayastor.list_nexus(Null {}).await?;
        Ok(())
    }

    #[tokio::test]
    async fn node() {
        init_tracing();
        let maya_name = NodeId::from("node-test-name");
        let test = Builder::new()
            .name("node")
            .add_container_bin(
                "nats",
                Binary::from_nix("nats-server").with_arg("-DV"),
            )
            .add_container_bin(
                "core",
                Binary::from_dbg("core")
                    .with_nats("-n")
                    .with_args(vec!["-d", "2sec"]),
            )
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", maya_name.as_str()]),
            )
            .autorun(false)
            .build()
            .await
            .unwrap();

        orderly_start(&test).await.unwrap();

        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node {
                id: maya_name.clone(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Online,
            }
        );
        tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
        let nodes = GetNodes {}.request().await.unwrap();
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node {
                id: maya_name.clone(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Offline,
            }
        );
    }
}
