use super::*;
use crate::core::{registry::Registry, wrapper::NodeWrapper};
use common::{
    errors::{GrpcRequestError, NodeNotFound, SvcError},
    v0::msg_translation::RpcToMessageBus,
};
use rpc::mayastor::ListBlockDevicesRequest;
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Node's Service
#[derive(Debug, Clone)]
pub(crate) struct Service {
    registry: Registry,
    /// deadline for receiving keepalive/Register messages
    deadline: std::time::Duration,
}

impl Service {
    /// New Node Service which uses the `registry` as its node cache and sets
    /// the `deadline` to each node's watchdog
    pub(super) fn new(
        registry: Registry,
        deadline: std::time::Duration,
    ) -> Self {
        Self {
            registry,
            deadline,
        }
    }

    /// Callback to be called when a node's watchdog times out
    pub(super) async fn on_timeout(service: &Service, id: &NodeId) {
        let registry = service.registry.clone();
        let state = registry.nodes.read().await;
        if let Some(node) = state.get(id) {
            let mut node = node.lock().await;
            if node.is_online() {
                tracing::error!(
                    "Node id '{}' missed the registration deadline of {:?}",
                    id,
                    service.deadline
                );
                node.update();
            }
        }
    }

    /// Register a new node through the register information
    pub(super) async fn register(&self, registration: &Register) {
        let node = Node {
            id: registration.id.clone(),
            grpc_endpoint: registration.grpc_endpoint.clone(),
            state: NodeState::Online,
        };
        let mut nodes = self.registry.nodes.write().await;
        match nodes.get_mut(&node.id) {
            None => {
                let mut node = NodeWrapper::new(&node, self.deadline);
                node.watchdog_mut().arm(self.clone());
                nodes.insert(node.id.clone(), Arc::new(Mutex::new(node)));
            }
            Some(node) => {
                node.lock().await.on_register().await;
            }
        }
    }

    /// Deregister a node through the deregister information
    pub(super) async fn deregister(&self, node: &Deregister) {
        let nodes = self.registry.nodes.read().await;
        match nodes.get(&node.id) {
            None => {}
            // ideally we want this node to disappear completely when it's not
            // part of the daemonset, but we just don't have that kind of
            // information at this level :(
            // maybe nodes should also be registered/deregistered via REST?
            Some(node) => {
                node.lock().await.set_state(NodeState::Unknown);
            }
        }
    }

    /// Get all nodes
    pub(crate) async fn get_nodes(
        &self,
        _: &GetNodes,
    ) -> Result<Nodes, SvcError> {
        let nodes = self.registry.get_nodes_wrapper().await;
        let mut nodes_vec = vec![];
        for node in nodes {
            nodes_vec.push(node.lock().await.node().clone());
        }
        Ok(Nodes(nodes_vec))
    }

    /// Get block devices from a node
    pub(crate) async fn get_block_devices(
        &self,
        request: &GetBlockDevices,
    ) -> Result<BlockDevices, SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;

        let grpc = node.lock().await.grpc_context()?;
        let mut client = grpc.connect().await?;

        let result = client
            .client
            .list_block_devices(ListBlockDevicesRequest {
                all: request.all,
            })
            .await;

        let response = result
            .context(GrpcRequestError {
                resource: ResourceKind::Block,
                request: "list_block_devices",
            })?
            .into_inner();

        let bdevs = response
            .devices
            .iter()
            .map(|rpc_bdev| rpc_bdev.to_mbus())
            .collect();
        Ok(BlockDevices(bdevs))
    }
}

impl Registry {
    /// Get all node wrappers
    pub(crate) async fn get_nodes_wrapper(
        &self,
    ) -> Vec<Arc<Mutex<NodeWrapper>>> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Get node `node_id`
    pub(crate) async fn get_node_wrapper(
        &self,
        node_id: &NodeId,
    ) -> Option<Arc<Mutex<NodeWrapper>>> {
        let nodes = self.nodes.read().await;
        match nodes.iter().find(|n| n.0 == node_id) {
            None => None,
            Some((_, node)) => Some(node.clone()),
        }
    }
}
