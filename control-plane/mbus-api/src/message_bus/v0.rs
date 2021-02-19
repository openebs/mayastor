// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

pub use crate::{v0::*, *};
use async_trait::async_trait;

/// Error sending/receiving
/// Common error type for send/receive
#[derive(Debug, Snafu, strum_macros::AsRefStr)]
#[allow(missing_docs)]
pub enum BusError {
    #[snafu(display("Bus Internal error"))]
    MessageBusError { source: Error },
    #[snafu(display("Resource not unique"))]
    NotUnique,
    #[snafu(display("Resource not found"))]
    NotFound,
}

impl From<Error> for BusError {
    fn from(source: Error) -> Self {
        BusError::MessageBusError {
            source,
        }
    }
}

/// Result for sending/receiving
pub type BusResult<T> = Result<T, BusError>;

macro_rules! only_one {
    ($list:ident) => {
        if let Some(obj) = $list.first() {
            if $list.len() > 1 {
                Err(BusError::NotUnique)
            } else {
                Ok(obj.clone())
            }
        } else {
            Err(BusError::NotFound)
        }
    };
}

/// Interface used by the rest service to interact with the control plane
/// services via the message bus
#[async_trait]
pub trait MessageBusTrait: Sized {
    /// Get all known nodes from the registry
    #[tracing::instrument(level = "debug", err)]
    async fn get_nodes() -> BusResult<Vec<Node>> {
        Ok(GetNodes {}.request().await?.into_inner())
    }

    /// Get node with `id`
    #[tracing::instrument(level = "debug", err)]
    async fn get_node(id: &NodeId) -> BusResult<Node> {
        let nodes = Self::get_nodes().await?;
        let nodes = nodes
            .into_iter()
            .filter(|n| &n.id == id)
            .collect::<Vec<_>>();
        only_one!(nodes)
    }

    /// Get pool with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_pool(filter: Filter) -> BusResult<Pool> {
        let pools = Self::get_pools(filter).await?;
        only_one!(pools)
    }

    /// Get pools with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_pools(filter: Filter) -> BusResult<Vec<Pool>> {
        let pools = GetPools {
            filter,
        }
        .request()
        .await?;
        Ok(pools.into_inner())
    }

    /// create pool
    #[tracing::instrument(level = "debug", err)]
    async fn create_pool(request: CreatePool) -> BusResult<Pool> {
        Ok(request.request().await?)
    }

    /// destroy pool
    #[tracing::instrument(level = "debug", err)]
    async fn destroy_pool(request: DestroyPool) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// Get replica with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_replica(filter: Filter) -> BusResult<Replica> {
        let replicas = Self::get_replicas(filter).await?;
        only_one!(replicas)
    }

    /// Get replicas with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_replicas(filter: Filter) -> BusResult<Vec<Replica>> {
        let replicas = GetReplicas {
            filter,
        }
        .request()
        .await?;
        Ok(replicas.into_inner())
    }

    /// create replica
    #[tracing::instrument(level = "debug", err)]
    async fn create_replica(request: CreateReplica) -> BusResult<Replica> {
        Ok(request.request().await?)
    }

    /// destroy replica
    #[tracing::instrument(level = "debug", err)]
    async fn destroy_replica(request: DestroyReplica) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// share replica
    #[tracing::instrument(level = "debug", err)]
    async fn share_replica(request: ShareReplica) -> BusResult<String> {
        Ok(request.request().await?)
    }

    /// unshare replica
    #[tracing::instrument(level = "debug", err)]
    async fn unshare_replica(request: UnshareReplica) -> BusResult<()> {
        let _ = request.request().await?;
        Ok(())
    }

    /// Get nexuses with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_nexuses(filter: Filter) -> BusResult<Vec<Nexus>> {
        let nexuses = GetNexuses {
            filter,
        }
        .request()
        .await?;
        Ok(nexuses.into_inner())
    }

    /// Get nexus with filter
    #[tracing::instrument(level = "debug", err)]
    async fn get_nexus(filter: Filter) -> BusResult<Nexus> {
        let nexuses = Self::get_nexuses(filter).await?;
        only_one!(nexuses)
    }

    /// create nexus
    #[tracing::instrument(level = "debug", err)]
    async fn create_nexus(request: CreateNexus) -> BusResult<Nexus> {
        Ok(request.request().await?)
    }

    /// destroy nexus
    #[tracing::instrument(level = "debug", err)]
    async fn destroy_nexus(request: DestroyNexus) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// share nexus
    #[tracing::instrument(level = "debug", err)]
    async fn share_nexus(request: ShareNexus) -> BusResult<String> {
        Ok(request.request().await?)
    }

    /// unshare nexus
    #[tracing::instrument(level = "debug", err)]
    async fn unshare_nexus(request: UnshareNexus) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// add nexus child
    #[tracing::instrument(level = "debug", err)]
    #[allow(clippy::unit_arg)]
    async fn add_nexus_child(request: AddNexusChild) -> BusResult<Child> {
        Ok(request.request().await?)
    }

    /// remove nexus child
    #[tracing::instrument(level = "debug", err)]
    #[allow(clippy::unit_arg)]
    async fn remove_nexus_child(request: RemoveNexusChild) -> BusResult<()> {
        request.request().await?;
        Ok(())
    }

    /// Generic JSON gRPC call
    #[tracing::instrument(level = "debug", err)]
    async fn json_grpc_call(
        request: JsonGrpcRequest,
    ) -> BusResult<serde_json::Value> {
        Ok(request.request().await?)
    }

    /// Get block devices on a node
    #[tracing::instrument(level = "debug", err)]
    async fn get_block_devices(
        request: GetBlockDevices,
    ) -> BusResult<BlockDevices> {
        Ok(request.request().await?)
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

        test_bus_backend(&NodeId::from(mayastor), &test).await?;

        // run with --nocapture to see all the logs
        test.logs_all().await?;
        Ok(())
    }

    async fn test_bus_backend(
        mayastor: &NodeId,
        test: &ComposeTest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nodes = MessageBus::get_nodes().await?;
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes.first().unwrap(),
            &Node {
                id: mayastor.clone(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Online,
            }
        );
        let node = MessageBus::get_node(mayastor).await?;
        assert_eq!(
            node,
            Node {
                id: mayastor.clone(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Online,
            }
        );

        test.stop("mayastor").await?;

        tokio::time::delay_for(std::time::Duration::from_millis(250)).await;
        assert!(MessageBus::get_nodes().await?.is_empty());

        Ok(())
    }
}
