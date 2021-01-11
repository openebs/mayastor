// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

use super::*;
use common::wrapper::v0::*;

/// Pool service implementation methods
#[derive(Clone, Debug, Default)]
pub(super) struct PoolSvc {
    registry: Registry<NodeWrapperPool>,
}

impl PoolSvc {
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

    /// Get all pools from node or from all nodes
    async fn get_node_pools(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Pool>, SvcError> {
        Ok(match node_id {
            None => self.registry.list_pools().await,
            Some(node_id) => self.registry.list_node_pools(&node_id).await,
        })
    }

    /// Get all replicas from node or from all nodes
    async fn get_node_replicas(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Replica>, SvcError> {
        Ok(match node_id {
            None => self.registry.list_replicas().await,
            Some(node_id) => self.registry.list_node_replicas(&node_id).await,
        })
    }

    /// Get pools according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_pools(
        &self,
        request: &GetPools,
    ) -> Result<Pools, SvcError> {
        let filter = request.filter.clone();
        Ok(Pools(match filter {
            Filter::None => self.get_node_pools(None).await?,
            Filter::Node(node_id) => self.get_node_pools(Some(node_id)).await?,
            Filter::NodePool(node_id, pool_id) => {
                let pools = self.get_node_pools(Some(node_id)).await?;
                pools.iter().filter(|&p| p.id == pool_id).cloned().collect()
            }
            Filter::Pool(pool_id) => {
                let pools = self.get_node_pools(None).await?;
                pools.iter().filter(|&p| p.id == pool_id).cloned().collect()
            }
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
        }))
    }

    /// Get replicas according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_replicas(
        &self,
        request: &GetReplicas,
    ) -> Result<Replicas, SvcError> {
        let filter = request.filter.clone();
        Ok(Replicas(match filter {
            Filter::None => self.get_node_replicas(None).await?,
            Filter::Node(node_id) => {
                self.get_node_replicas(Some(node_id)).await?
            }
            Filter::NodePool(node_id, pool_id) => {
                let replicas = self.get_node_replicas(Some(node_id)).await?;
                replicas
                    .iter()
                    .filter(|&p| p.pool == pool_id)
                    .cloned()
                    .collect()
            }
            Filter::Pool(pool_id) => {
                let replicas = self.get_node_replicas(None).await?;
                replicas
                    .iter()
                    .filter(|&p| p.pool == pool_id)
                    .cloned()
                    .collect()
            }
            Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
                let replicas = self.get_node_replicas(Some(node_id)).await?;
                replicas
                    .iter()
                    .filter(|&p| p.pool == pool_id && p.uuid == replica_id)
                    .cloned()
                    .collect()
            }
            Filter::NodeReplica(node_id, replica_id) => {
                let replicas = self.get_node_replicas(Some(node_id)).await?;
                replicas
                    .iter()
                    .filter(|&p| p.uuid == replica_id)
                    .cloned()
                    .collect()
            }
            Filter::PoolReplica(pool_id, replica_id) => {
                let replicas = self.get_node_replicas(None).await?;
                replicas
                    .iter()
                    .filter(|&p| p.pool == pool_id && p.uuid == replica_id)
                    .cloned()
                    .collect()
            }
            Filter::Replica(replica_id) => {
                let replicas = self.get_node_replicas(None).await?;
                replicas
                    .iter()
                    .filter(|&p| p.uuid == replica_id)
                    .cloned()
                    .collect()
            }
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
        }))
    }

    /// Create replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        self.registry.create_replica(&request).await
    }

    /// Destroy replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError> {
        self.registry.destroy_replica(&request).await
    }

    /// Share replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        self.registry.share_replica(&request).await
    }

    /// Unshare replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        self.registry.unshare_replica(&request).await
    }

    /// Create pool
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_pool(
        &self,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        self.registry.create_pool(request).await
    }

    /// Destroy pool
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_pool(
        &self,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        self.registry.destroy_pool(request).await
    }
}
