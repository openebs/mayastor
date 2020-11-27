use crate::{v0::*, *};
use async_trait::async_trait;

/// Error sending/receiving
pub type Error = crate::Error;
/// Result for sending/receiving
pub type BusResult<T> = crate::BusResult<T>;

/// Mayastor Node
pub type Node = crate::v0::Node;

/// Interface used by the rest service to interact with the control plane
/// services via the message bus
#[async_trait]
pub trait MessageBusTrait: Sized {
    /// Get all known nodes from the registry
    #[tracing::instrument(level = "info")]
    async fn get_nodes() -> BusResult<Vec<Node>> {
        GetNodes {}.request().await.map(|v| v.0)
    }
    /// Get a node through its id
    #[tracing::instrument(level = "info")]
    async fn get_node(id: String) -> BusResult<Option<Node>> {
        let nodes = Self::get_nodes().await?;
        Ok(nodes.into_iter().find(|n| n.id == id))
    }
}

/// Implementation of the bus interface trait
pub struct MessageBus {}
impl MessageBusTrait for MessageBus {}

#[cfg(test)]
mod tests {
    use super::*;
    use composer::*;
    use rpc::mayastor::Null;

    async fn bus_init() -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            crate::message_bus_init("10.1.0.2".into()).await
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
        test.start_containers(vec!["nats", "node"]).await?;

        bus_init().await?;
        wait_for_node().await?;

        test.start("mayastor").await?;

        let mut hdl = test.grpc_handle("mayastor").await?;
        hdl.mayastor.list_nexus(Null {}).await?;
        Ok(())
    }

    #[tokio::test]
    async fn bus() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();
        let mayastor = "node-test-name";
        let test = Builder::new()
            .name("rest_backend")
            .add_container_bin(
                "nats",
                Binary::from_nix("nats-server").with_arg("-DV"),
            )
            .add_container_bin("node", Binary::from_dbg("node").with_nats("-n"))
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", mayastor]),
            )
            .with_clean(true)
            .autorun(false)
            .build()
            .await?;

        orderly_start(&test).await?;

        test_bus_backend(mayastor, &test).await?;

        // run with --nocapture to see all the logs
        test.logs_all().await?;
        Ok(())
    }

    async fn test_bus_backend(
        mayastor: &str,
        test: &ComposeTest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nodes = MessageBus::get_nodes().await?;
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes.first().unwrap(),
            &Node {
                id: mayastor.to_string(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Online,
            }
        );
        let node = MessageBus::get_node(mayastor.to_string()).await?;
        assert_eq!(
            node,
            Some(Node {
                id: mayastor.to_string(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Online,
            })
        );

        test.stop("mayastor").await?;

        tokio::time::delay_for(std::time::Duration::from_millis(250)).await;
        assert!(MessageBus::get_nodes().await?.is_empty());

        Ok(())
    }
}
