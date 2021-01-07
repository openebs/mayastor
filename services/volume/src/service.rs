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

    /// Get volumes
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_volumes(
        &self,
        request: &GetVolumes,
    ) -> Result<Volumes, SvcError> {
        let nexus = self.registry.list_nexuses().await;
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
        let pools = self.registry.fetch_pools_wrapper().await;

        let size = request.size;
        let replicas = request.replicas;
        let allowed_nodes = request.allowed_nodes.clone();

        if !allowed_nodes.is_empty() && replicas > allowed_nodes.len() as u64 {
            // oops, how would this even work mr requester?
            return Err(SvcError::InvalidArguments {});
        }

        if request.nexuses > 1 {
            tracing::warn!(
                "Multiple nexus per volume is not currently working"
            );
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
                allowed_nodes.is_empty() || allowed_nodes.contains(&p.node())
            })
            .filter(|&p| {
                // enough free space
                p.free_space() >= size
            })
            .filter(|&p| {
                // but preferably (the sort will sort this out for us)
                p.state() != PoolState::Faulted
                    && p.state() != PoolState::Unknown
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
                node: pool.node(),
                uuid: ReplicaId::from(request.uuid.as_str()),
                pool: pool.uuid(),
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
            let replica = self.registry.create_replica(&create_replica).await;
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

                match self.registry.create_nexus(&create_nexus).await {
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
        let nexuses = self.registry.list_nexuses().await;
        let nexuses = nexuses
            .iter()
            .filter(|n| n.uuid.as_str() == request.uuid.as_str())
            .collect::<Vec<_>>();
        for nexus in nexuses {
            self.registry
                .destroy_nexus(&DestroyNexus {
                    node: nexus.node.clone(),
                    uuid: NexusId::from(request.uuid.as_str()),
                })
                .await?;
            for child in &nexus.children {
                let replicas = self.registry.list_replicas().await;
                let replica = replicas
                    .iter()
                    .find(|r| r.uri.as_str() == child.uri.as_str());
                if let Some(replica) = replica {
                    self.registry
                        .destroy_replica(&DestroyReplica {
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
