use crate::core::{registry::Registry, wrapper::ClientOps};
use common::errors::{NodeNotFound, NotEnough, SvcError};
use mbus_api::{
    v0::{
        AddNexusChild,
        Child,
        CreateNexus,
        CreateReplica,
        CreateVolume,
        DestroyNexus,
        DestroyReplica,
        DestroyVolume,
        Filter,
        GetNexuses,
        GetVolumes,
        Nexus,
        NexusId,
        NexusState,
        Nexuses,
        PoolState,
        Protocol,
        RemoveNexusChild,
        ReplicaId,
        ShareNexus,
        UnshareNexus,
        Volume,
        VolumeId,
        Volumes,
    },
    ErrorChain,
};
use snafu::OptionExt;

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            registry,
        }
    }

    /// Get nexuses according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_nexuses(
        &self,
        request: &GetNexuses,
    ) -> Result<Nexuses, SvcError> {
        let filter = request.filter.clone();
        let nexuses = match filter {
            Filter::None => self.registry.get_node_opt_nexuses(None).await?,
            Filter::Node(node_id) => {
                self.registry.get_node_nexuses(&node_id).await?
            }
            Filter::NodeNexus(node_id, nexus_id) => {
                let nexus =
                    self.registry.get_node_nexus(&node_id, &nexus_id).await?;
                vec![nexus]
            }
            Filter::Nexus(nexus_id) => {
                let nexus = self.registry.get_nexus(&nexus_id).await?;
                vec![nexus]
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
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.create_nexus(request).await
    }

    /// Destroy nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.destroy_nexus(request).await
    }

    /// Share nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.share_nexus(request).await
    }

    /// Unshare nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.unshare_nexus(request).await
    }

    /// Add nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn add_nexus_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.add_child(request).await
    }

    /// Remove nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let node = self
            .registry
            .get_node_wrapper(&request.node)
            .await
            .context(NodeNotFound {
                node_id: request.node.clone(),
            })?;
        node.remove_child(request).await
    }

    /// Get volumes
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_volumes(
        &self,
        request: &GetVolumes,
    ) -> Result<Volumes, SvcError> {
        let nexus = self.registry.get_nexuses().await;
        Ok(Volumes(
            nexus
                .iter()
                .map(|n| Volume {
                    uuid: VolumeId::from(n.uuid.as_str()),
                    size: n.size,
                    state: n.state.clone(),
                    children: vec![n.clone()],
                })
                .collect(),
        ))
    }

    /// Create volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_volume(
        &self,
        request: &CreateVolume,
    ) -> Result<Volume, SvcError> {
        // should we just use the cache here?
        let pools = self.registry.get_pools_wrapper().await?;

        let size = request.size;
        let replicas = request.replicas;
        let allowed_nodes = request.allowed_nodes.clone();

        if !allowed_nodes.is_empty() && replicas > allowed_nodes.len() as u64 {
            // oops, how would this even work mr requester?
            return Err(SvcError::InvalidArguments {});
        }

        if request.nexuses > 1 {
            panic!("ANA volumes is not currently supported");
        }

        // filter pools according to the following criteria (any order):
        // 1. if allowed_nodes were specified then only pools from those nodes
        // can be used.
        // 2. pools should have enough free space for the
        // volume (do we need to take into account metadata?)
        // 3. ideally use only healthy(online) pools with degraded pools as a
        // fallback
        let mut pools = pools
            .iter()
            .filter(|&p| {
                // required nodes, if any
                allowed_nodes.is_empty() || allowed_nodes.contains(&p.node)
            })
            .filter(|&p| {
                // enough free space
                p.free_space() >= size
            })
            .filter(|&p| {
                // but preferably (the sort will sort this out for us)
                p.state != PoolState::Faulted && p.state != PoolState::Unknown
            })
            .collect::<Vec<_>>();

        // we could not satisfy the request, no point in continuing any further
        if replicas > pools.len() as u64 {
            return Err(NotEnough::OfPools {
                have: pools.len() as u64,
                need: replicas,
            }
            .into());
        }

        // sort pools from least to most suitable
        // state and then number of replicas and then free space
        pools.sort();

        let mut replicas = vec![];
        while let Some(pool) = pools.pop() {
            let create_replica = CreateReplica {
                node: pool.node.clone(),
                uuid: ReplicaId::from(request.uuid.as_str()),
                pool: pool.id.clone(),
                size: request.size,
                thin: true,
                share: if replicas.is_empty() {
                    // one 1 nexus supported for the moment which will use
                    // replica 0
                    Protocol::Off
                } else {
                    // the others will fail to create because they can't open
                    // their local replica via Nvmf
                    Protocol::Nvmf
                },
            };
            let node = self
                .registry
                .get_node_wrapper(&create_replica.node)
                .await
                .context(NodeNotFound {
                    node_id: create_replica.node.clone(),
                })?;
            let replica = node.create_replica(&create_replica).await;
            if let Ok(replica) = replica {
                replicas.push(replica);
            } else {
                tracing::error!(
                    "Failed to create replica: {:?}. Trying other pools (if any available)...",
                    create_replica
                );
            }

            if replicas.len() == request.replicas as usize {
                break;
            }
        }

        if replicas.len() == request.replicas as usize {
            // we have enough replicas
            // now stitch them up and make up the nexuses
            // where are the nexuses allowed to exist?
            // (at the moment on the same nodes as the most preferred replicas)

            let mut nexuses = vec![];
            for i in 0 .. request.nexuses {
                let create_nexus = CreateNexus {
                    node: replicas[i as usize].node.clone(),
                    uuid: NexusId::from(request.uuid.as_str()),
                    size: request.size,
                    children: replicas
                        .iter()
                        .map(|r| r.uri.to_string().into())
                        .collect(),
                };

                match self.create_nexus(&create_nexus).await {
                    Ok(nexus) => {
                        nexuses.push(nexus);
                    }
                    Err(error) => {
                        // what to do in case of failure?
                        tracing::error!(
                            "Failed to create nexus: {:?}, error: {}",
                            create_nexus,
                            error.full_string()
                        );
                    }
                }
            }

            if nexuses.is_empty() {
                Err(NotEnough::OfNexuses {
                    have: 0,
                    need: 1,
                }
                .into())
            } else {
                let volume = Volume {
                    uuid: request.uuid.clone(),
                    size: request.size,
                    state: NexusState::Online,
                    children: nexuses,
                };
                Ok(volume)
            }
        } else {
            // we can't fulfil the request fully...
            // carry on to a "degraded" state with "enough" replicas or bail
            // out?
            Err(NotEnough::OfReplicas {
                have: replicas.len() as u64,
                need: request.replicas,
            }
            .into())
        }
    }

    /// Destroy volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_volume(
        &self,
        request: &DestroyVolume,
    ) -> Result<(), SvcError> {
        let nexuses = self.registry.get_nexuses().await;
        let nexuses = nexuses
            .iter()
            .filter(|n| n.uuid.as_str() == request.uuid.as_str())
            .collect::<Vec<_>>();

        for nexus in nexuses {
            self.destroy_nexus(&DestroyNexus {
                node: nexus.node.clone(),
                uuid: NexusId::from(request.uuid.as_str()),
            })
            .await?;
            for child in &nexus.children {
                let replicas = self.registry.get_replicas().await?;
                let replica = replicas
                    .iter()
                    .find(|r| r.uri.as_str() == child.uri.as_str());
                if let Some(replica) = replica {
                    let node = self
                        .registry
                        .get_node_wrapper(&replica.node)
                        .await
                        .context(NodeNotFound {
                            node_id: replica.node.clone(),
                        })?;
                    node.destroy_replica(&DestroyReplica {
                        node: replica.node.clone(),
                        pool: replica.pool.clone(),
                        uuid: replica.uuid.clone(),
                    })
                    .await?;
                }
            }
        }
        Ok(())
    }
}
