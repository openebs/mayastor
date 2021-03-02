use crate::core::{registry::Registry, wrapper::*};
use common::errors::{NodeNotFound, PoolNotFound, ReplicaNotFound, SvcError};
use mbus_api::v0::{NodeId, Pool, PoolId, Replica, ReplicaId};
use snafu::OptionExt;

/// Pool helpers
impl Registry {
    /// Get all pools from node `node_id` or from all nodes
    pub(crate) async fn get_node_opt_pools(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Pool>, SvcError> {
        match node_id {
            None => self.get_pools_inner().await,
            Some(node_id) => self.get_node_pools(&node_id).await,
        }
    }

    /// Get wrapper pool `pool_id` from node `node_id`
    pub(crate) async fn get_node_pool_wrapper(
        &self,
        node_id: &NodeId,
        pool_id: &PoolId,
    ) -> Result<PoolWrapper, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        let pool = node.pool(pool_id).await.context(PoolNotFound {
            pool_id: pool_id.clone(),
        })?;
        Ok(pool)
    }

    /// Get pool wrapper for `pool_id`
    pub(crate) async fn get_pool_wrapper(
        &self,
        pool_id: &PoolId,
    ) -> Result<PoolWrapper, SvcError> {
        let nodes = self.get_nodes_wrapper().await;
        for node in nodes {
            if let Some(pool) = node.pool(pool_id).await {
                return Ok(pool);
            }
        }
        Err(common::errors::SvcError::PoolNotFound {
            pool_id: pool_id.clone(),
        })
    }

    /// Get all pool wrappers
    pub(crate) async fn get_pools_wrapper(
        &self,
    ) -> Result<Vec<PoolWrapper>, SvcError> {
        let nodes = self.get_nodes_wrapper().await;
        let mut pools = vec![];
        for node in nodes {
            pools.extend(node.pools().await);
        }
        Ok(pools)
    }

    /// Get all pools
    pub(crate) async fn get_pools_inner(&self) -> Result<Vec<Pool>, SvcError> {
        let nodes = self.get_pools_wrapper().await?;
        Ok(nodes.iter().map(Pool::from).collect())
    }

    /// Get all pools from node `node_id`
    pub(crate) async fn get_node_pools(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<Pool>, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        Ok(node.pools().await.iter().map(Pool::from).collect())
    }
}

/// Replica helpers
impl Registry {
    /// Get all replicas from node `node_id` or from all nodes
    pub(crate) async fn get_node_opt_replicas(
        &self,
        node_id: Option<NodeId>,
    ) -> Result<Vec<Replica>, SvcError> {
        match node_id {
            None => self.get_replicas().await,
            Some(node_id) => self.get_node_replicas(&node_id).await,
        }
    }

    /// Get all replicas
    pub(crate) async fn get_replicas(&self) -> Result<Vec<Replica>, SvcError> {
        let nodes = self.get_pools_wrapper().await?;
        Ok(nodes.iter().map(|pool| pool.replicas()).flatten().collect())
    }

    /// Get replica `replica_id`
    pub(crate) async fn get_replica(
        &self,
        replica_id: &ReplicaId,
    ) -> Result<Replica, SvcError> {
        let replicas = self.get_replicas().await?;
        let replica = replicas.iter().find(|r| &r.uuid == replica_id).context(
            ReplicaNotFound {
                replica_id: replica_id.clone(),
            },
        )?;
        Ok(replica.clone())
    }

    /// Get all replicas from node `node_id`
    pub(crate) async fn get_node_replicas(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<Replica>, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        Ok(node.replicas().await)
    }

    /// Get replica `replica_id` from node `node_id`
    pub(crate) async fn get_node_replica(
        &self,
        node_id: &NodeId,
        replica_id: &ReplicaId,
    ) -> Result<Replica, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        let replica =
            node.replica(replica_id).await.context(ReplicaNotFound {
                replica_id: replica_id.clone(),
            })?;
        Ok(replica)
    }

    /// Get replica `replica_id` from pool `pool_id`
    pub(crate) async fn get_pool_replica(
        &self,
        pool_id: &PoolId,
        replica_id: &ReplicaId,
    ) -> Result<Replica, SvcError> {
        let pool = self.get_pool_wrapper(pool_id).await?;
        let replica = pool.replica(replica_id).context(ReplicaNotFound {
            replica_id: replica_id.clone(),
        })?;
        Ok(replica.clone())
    }

    /// Get replica `replica_id` from pool `pool_id` on node `node_id`
    pub(crate) async fn get_node_pool_replica(
        &self,
        node_id: &NodeId,
        pool_id: &PoolId,
        replica_id: &ReplicaId,
    ) -> Result<Replica, SvcError> {
        let node =
            self.get_node_wrapper(node_id).await.context(NodeNotFound {
                node_id: node_id.clone(),
            })?;
        let pool = node.pool(pool_id).await.context(PoolNotFound {
            pool_id: pool_id.clone(),
        })?;
        let replica = pool.replica(replica_id).context(ReplicaNotFound {
            replica_id: replica_id.clone(),
        })?;
        Ok(replica.clone())
    }
}
