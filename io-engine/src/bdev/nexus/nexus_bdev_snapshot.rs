//! Implements snapshot operations on a nexus.
use std::collections::{HashMap, HashSet};

use futures::future::join_all;

use super::{Error, Nexus, NexusOperation, NexusState};
use crate::{
    bdev::nexus::NexusChild,
    core::{
        snapshot::SnapshotDescriptor,
        BlockDeviceHandle,
        IntoErrno,
        SnapshotParams,
    },
};
use chrono::{DateTime, Utc};
use std::pin::Pin;
/// Per-replica descriptor for nexus snapshot operation.
#[derive(Debug)]
pub struct NexusReplicaSnapshotDescriptor {
    pub replica_uuid: String,
    pub snapshot_uuid: Option<String>,
    pub skip: bool,
}

/// Per-replica status of a nexus snapshot operation.
#[derive(Debug)]
pub struct NexusReplicaSnapshotStatus {
    pub replica_uuid: String,
    pub status: u32,
}

/// Status of a nexus snapshot operation.
#[derive(Debug)]
pub struct NexusSnapshotStatus {
    pub snapshot_timestamp: Option<DateTime<Utc>>,
    pub replicas_done: Vec<NexusReplicaSnapshotStatus>,
    pub replicas_skipped: Vec<String>,
}

/// Driver for performing snapshot creation on multiple nexus replicas in
/// parallel.
struct ReplicaSnapshotExecutor {
    /// Replica UUID -> Snapshot UUID map.
    replica_ctx: Vec<SnapshotExecutorReplicaCtx>,
    skipped_replicas: Vec<String>,
}

struct SnapshotExecutorReplicaCtx {
    snapshot_uuid: String,
    replica_uuid: String,
    handle: Box<dyn BlockDeviceHandle>,
}

impl ReplicaSnapshotExecutor {
    /// Create a snapshot executor based on snapshot config and replica
    /// topology.
    async fn new(
        nexus: Pin<&'_ Nexus<'_>>,
        replicas: Vec<NexusReplicaSnapshotDescriptor>,
    ) -> Result<Self, Error> {
        // Make sure requested replicas match nexus's topology.
        // Number replicas in nexus must match the number of replicas
        // participating in snapshot operation, though some replicas can
        // be explicitly omitted.
        if nexus.children().len() != replicas.len() {
            return Err(Error::FailedCreateSnapshot {
                name: nexus.bdev_name(),
                reason: format!(
                    "Snapshot topology doesn't match nexus {} topology: nexus replicas={}, snapshot replicas={}",
                    nexus.bdev_name(),
                    nexus.children().len(),
                    replicas.len(),
                )
            });
        }

        // Make sure no duplicated replicas are provided and all replicas match
        // existing nexus replicas.
        let nexus_replicas: HashMap<String, &NexusChild> = nexus
            .children()
            .iter()
            .filter_map(|c| c.get_uuid().map(|u| (u, c)))
            .collect();

        let mut seen_replicas: HashSet<String> = HashSet::new();
        let mut replica_ctx: Vec<SnapshotExecutorReplicaCtx> = Vec::new();
        let mut skipped_replicas = Vec::new();

        for r in &replicas {
            let replica = match nexus_replicas.get(&r.replica_uuid) {
                Some(c) => {
                    // Make sure target replica appers only once.
                    if seen_replicas.contains(&r.replica_uuid) {
                        return Err(Error::FailedCreateSnapshot {
                            name: nexus.bdev_name(),
                            reason: format!(
                                "Duplicated replica {}",
                                &r.replica_uuid,
                            ),
                        });
                    }
                    seen_replicas.insert(r.replica_uuid.to_string());

                    *c
                }
                None => {
                    return Err(Error::FailedCreateSnapshot {
                        name: nexus.bdev_name(),
                        reason: format!(
                            "Nexus {}, does not contain replica with UUID {}",
                            nexus.bdev_name(),
                            &r.replica_uuid,
                        ),
                    })
                }
            };

            if !r.skip {
                // Replica must be healthy for a snapshot to be taken.
                if !replica.is_healthy() {
                    return Err(Error::FailedCreateSnapshot {
                        name: nexus.bdev_name(),
                        reason: format!(
                            "Replica {} is not healthy",
                            &r.replica_uuid,
                        ),
                    });
                }

                // Snapshot UUID must be provided if the replica is not
                // explicitly skipped.
                let snapshot_uuid = match &r.snapshot_uuid {
                    Some(s) => s.to_owned(),
                    None => {
                        return Err(Error::FailedCreateSnapshot {
                            name: nexus.bdev_name(),
                            reason: format!(
                                "Snapshot UUID is missing for replica {}",
                                &r.replica_uuid,
                            ),
                        })
                    }
                };

                let handle = replica
                    .get_io_handle_nonblock()
                    .await
                    .map_err(|error| {
                        error!(
                            ?replica,
                            ?error,
                            "Failed to get I/O handle for replica, nexus snapshot creation failed"
                        );
                        Error::FailedGetHandle {}
                    })?;

                replica_ctx.push(SnapshotExecutorReplicaCtx {
                    replica_uuid: r.replica_uuid.clone(),
                    snapshot_uuid,
                    handle,
                });
            } else {
                skipped_replicas.push(r.replica_uuid.clone());
            }
        }

        Ok(Self {
            replica_ctx,
            skipped_replicas,
        })
    }

    /// Take snapshots for all replicas participating in the operation.
    async fn take_snapshot(
        &self,
        snapshot: &SnapshotParams,
    ) -> (Vec<NexusReplicaSnapshotStatus>, Vec<String>) {
        let futures = self
            .replica_ctx
            .iter()
            .map(|ctx| {
                // Replica snapshot should have its own, user-defined GUID,
                // whilst preserving other properties shared among other
                // snapshots.
                let snapshot_params = SnapshotParams::new(
                    snapshot.entity_id(),
                    Some(ctx.replica_uuid.clone()),
                    snapshot.txn_id(),
                    snapshot.name(),
                    Some(ctx.snapshot_uuid.clone()),
                    snapshot.create_time(),
                );
                let replica_uuid = ctx.replica_uuid.clone();
                debug!(
                    replica_uuid,
                    ?snapshot_params,
                    "Starting nexus replica snapshot operation",
                );

                // Snapshot operation future shall be able to track back to the
                // replica.
                async move {
                    (
                        replica_uuid,
                        ctx.handle.create_snapshot(snapshot_params).await,
                    )
                }
            })
            .collect::<Vec<_>>();

        let result = join_all(futures).await;

        let res = result
            .into_iter()
            .map(|(u, r)| {
                // Transform snapshot operation status into errno.
                let status = r.map_or_else(|e| e.into_errno(), |_r| 0);

                NexusReplicaSnapshotStatus {
                    replica_uuid: u,
                    status,
                }
            })
            .collect::<Vec<_>>();

        (res, self.skipped_replicas.clone())
    }
}

impl<'n> Nexus<'n> {
    fn check_nexus_state(&self) -> Result<(), Error> {
        self.check_nexus_operation(NexusOperation::NexusSnapshot)?;

        // Check that nexus has exactly 1 replica.
        match self.children().len() {
            0 => {
                return Err(Error::FailedCreateSnapshot {
                    name: self.bdev_name(),
                    reason: "Nexus has no replicas".to_string(),
                })
            }
            1 => {} // Only one replica nexuses are supported.
            _ => {
                return Err(Error::FailedCreateSnapshot {
                    name: self.bdev_name(),
                    reason: "Nexus has more than one replica".to_string(),
                })
            }
        }

        // Check that nexus is healthy and not being reconfigured.
        let state = *self.state.lock();
        if state != NexusState::Open {
            return Err(Error::FailedCreateSnapshot {
                name: self.bdev_name(),
                reason: "Nexus is not opened".to_string(),
            });
        }

        Ok(())
    }

    /// Create a snapshot on all nexus replicas (currently only on 1)
    async fn do_nexus_snapshot(
        self: Pin<&mut Self>,
        snapshot: SnapshotParams,
        replicas: Vec<NexusReplicaSnapshotDescriptor>,
    ) -> Result<NexusSnapshotStatus, Error> {
        let (replicas_done, replicas_skipped) =
            ReplicaSnapshotExecutor::new(self.as_ref(), replicas)
                .await?
                .take_snapshot(&snapshot)
                .await;
        Ok(NexusSnapshotStatus {
            replicas_done,
            replicas_skipped,
            snapshot_timestamp: snapshot
                .create_time()
                .map(|t| t.parse::<DateTime<Utc>>().unwrap_or_default()),
        })
    }

    /// Create a snapshot on all children
    pub async fn create_snapshot(
        mut self: Pin<&mut Self>,
        snapshot: SnapshotParams,
        replicas: Vec<NexusReplicaSnapshotDescriptor>,
    ) -> Result<NexusSnapshotStatus, Error> {
        if snapshot.name().is_none() {
            return Err(Error::FailedCreateSnapshot {
                name: self.bdev_name(),
                reason: "Snapshot name must be provided".to_string(),
            });
        }

        self.check_nexus_state()?;

        // For now only single replica nexus is supported.
        if self.children_iter().len() != 1 {
            return Err(Error::FailedCreateSnapshot {
                name: self.bdev_name(),
                reason: "Nexus has more than one replica".to_string(),
            });
        }

        // Step 1: Pause I/O subsystem for nexus.
        self.as_mut().pause().await.map_err(|error| {
            error!(
                ?self,
                ?error,
                "Failed to pause I/O subsystem, nexus snapshot creation failed"
            );
            error
        })?;

        // Step 2: Create snapshots on all replicas.
        let res = self.as_mut().do_nexus_snapshot(snapshot, replicas).await;

        // Step 3: Resume I/O.
        if let Err(error) = self.as_mut().resume().await {
            error!(
                ?self,
                ?error,
                "Failed to unpause nexus I/O subsystem, nexus might be not accessible by initiator"
            );
        }

        res
    }
}
