#![allow(clippy::unit_arg)]

use super::*;
use common::wrapper::v0::*;

/// Volume service implementation methods
#[derive(Clone, Debug, Default)]
pub(super) struct VolumeSvc {
    registry: Registry<NodeWrapperVolume>,
}

impl VolumeSvc {
    /// New Service with the update `period`
    pub fn new(period: std::time::Duration) -> Self {
        let obj = Self {
            registry: Registry::new(period),
        };
        obj.start();
        obj
    }
    /// Start registry poller
    fn start(&self) {
        self.registry.start();
    }

    /// Get all nexuses from node or from all nodes
    async fn get_node_nexuses(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Nexus>, SvcError> {
        Ok(match node_id {
            None => self.registry.list_nexuses().await,
            Some(node_id) => self.registry.list_node_nexuses(&node_id).await,
        })
    }

    /// Get nexuses according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_nexuses(
        &self,
        request: &GetNexuses,
    ) -> Result<Nexuses, SvcError> {
        let filter = request.filter.clone();
        let nexuses = match filter {
            Filter::None => self.get_node_nexuses(None).await?,
            Filter::Node(node_id) => {
                self.get_node_nexuses(Some(node_id)).await?
            }
            Filter::NodeNexus(node_id, nexus_id) => {
                let nexuses = self.get_node_nexuses(Some(node_id)).await?;
                nexuses
                    .iter()
                    .filter(|&n| n.uuid == nexus_id)
                    .cloned()
                    .collect()
            }
            Filter::Nexus(nexus_id) => {
                let nexuses = self.get_node_nexuses(None).await?;
                nexuses
                    .iter()
                    .filter(|&n| n.uuid == nexus_id)
                    .cloned()
                    .collect()
            }
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
        };
        Ok(Nexuses(nexuses))
    }

    /// Create nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        self.registry.create_nexus(request).await
    }

    /// Destroy nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        self.registry.destroy_nexus(request).await
    }

    /// Share nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        self.registry.share_nexus(request).await
    }

    /// Unshare nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        self.registry.unshare_nexus(request).await
    }

    /// Add nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn add_nexus_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        self.registry.add_nexus_child(request).await
    }

    /// Remove nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        self.registry.remove_nexus_child(request).await
    }
}
