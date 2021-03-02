use crate::core::{registry::Registry, wrapper::*};
use common::errors::{NexusNotFound, NodeNotFound, SvcError};
use mbus_api::v0::{Nexus, NexusId, NodeId};
use snafu::OptionExt;

/// Nexus helpers
impl Registry {
    /// Get all nexuses from node `node_id` or from all nodes
    pub(crate) async fn get_node_opt_nexuses(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Nexus>, SvcError> {
        Ok(match node_id {
            None => self.get_nexuses().await,
            Some(node_id) => self.get_node_nexuses(&node_id).await?,
        })
    }

    /// Get all nexuses from node `node_id`
    pub(crate) async fn get_node_nexuses(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<Nexus>, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        Ok(node.nexuses().await)
    }

    /// Get nexus `nexus_id` from node `node_id`
    pub(crate) async fn get_node_nexus(
        &self,
        node_id: &NodeId,
        nexus_id: &NexusId,
    ) -> Result<Nexus, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        let nexus = node.nexus(nexus_id).await.context(NexusNotFound {
            nexus_id: nexus_id.clone(),
        })?;
        Ok(nexus)
    }

    /// Get nexus `nexus_id`
    pub(crate) async fn get_nexus(
        &self,
        nexus_id: &NexusId,
    ) -> Result<Nexus, SvcError> {
        let nodes = self.get_nodes_wrapper().await;
        for node in nodes {
            if let Some(nexus) = node.nexus(nexus_id).await {
                return Ok(nexus);
            }
        }
        Err(common::errors::SvcError::NexusNotFound {
            nexus_id: nexus_id.to_string(),
        })
    }

    /// Get all nexuses
    pub(crate) async fn get_nexuses(&self) -> Vec<Nexus> {
        let nodes = self.get_nodes_wrapper().await;
        let mut nexuses = vec![];
        for node in nodes {
            nexuses.extend(node.nexuses().await);
        }
        nexuses
    }
}
