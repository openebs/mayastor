use super::{IoMode, Nexus, NexusChild};
use crate::{persistent_store::PersistentStore, sleep::mayastor_sleep};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::Error;

/// Information associated with the persisted NexusInfo structure.
pub struct PersistentNexusInfo {
    /// Structure that is written to the persistent store.
    inner: NexusInfo,
    /// Key to use to persist the NexusInfo structure.
    /// If `Some` the key has been supplied by the control plane.
    key: Option<String>,
}

impl PersistentNexusInfo {
    /// Create a new instance of PersistentNexusInfo.
    pub(crate) fn new(key: Option<String>) -> Self {
        Self {
            inner: Default::default(),
            key,
        }
    }

    /// Get a mutable reference to the inner NexusInfo structure.
    fn inner_mut(&mut self) -> &mut NexusInfo {
        &mut self.inner
    }
}

/// Definition of the nexus information that gets saved in the persistent
/// store.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct NexusInfo {
    /// Nexus destroyed successfully.
    pub clean_shutdown: bool,
    /// Information about children.
    pub children: Vec<ChildInfo>,
}

/// Definition of the child information that gets saved in the persistent
/// store.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ChildInfo {
    /// UUID of the child.
    pub uuid: String,
    /// Child's state of health.
    pub healthy: bool,
}

/// Defines the type of persist operations.
pub(crate) enum PersistOp<'a> {
    /// Create a persistent entry.
    Create,
    /// Add a child to an existing persistent entry.
    AddChild { child_uri: String, healthy: bool },
    /// Remove a child from an existing persistent entry.
    RemoveChild { child_uri: String },
    /// Update a persistent entry.
    Update { child_uri: String, healthy: bool },
    /// Update a persistent entry only when a precondition on this NexusInfo
    /// holds. Predicate is called under protection of the NexusInfo lock,
    /// so the check is assumed to be atomic and not interfering with other
    /// modifications of the same NexusInfo.
    UpdateCond {
        child_uri: String,
        healthy: bool,
        predicate: &'a dyn Fn(&NexusInfo) -> bool,
    },
    /// Save the clean shutdown variable.
    Shutdown,
}

impl<'n> Nexus<'n> {
    /// Persists nexus's information to the store.
    pub(crate) async fn persist(&self, op: PersistOp<'_>) -> Result<(), Error> {
        if !PersistentStore::enabled() {
            return Ok(());
        }

        let mut persistent_nexus_info = self.nexus_info.lock().await;

        // We have to freeze I/O (re-)submissions while doing that, to prevent
        // an uncontrollable storm of I/O resubmissions in the case
        // the persistent store is slow to response, or has failed.
        self.set_nexus_io_mode(IoMode::Freeze).await;

        let nexus_info = persistent_nexus_info.inner_mut();

        match &op {
            PersistOp::Create => {
                // Initialisation of the persistent info will overwrite any
                // existing entries.
                // This should only be called on nexus creation, therefore we
                // expect the NexusInfo structure to contain default values.
                assert!(nexus_info.children.is_empty());
                assert!(!nexus_info.clean_shutdown);
                self.children_iter().for_each(|c| {
                    let child_info = ChildInfo {
                        uuid: NexusChild::uuid(c.uri())
                            .expect("Failed to get child UUID."),
                        healthy: c.is_healthy(),
                    };
                    nexus_info.children.push(child_info);
                });
            }
            PersistOp::AddChild {
                child_uri,
                healthy,
            } => {
                // Add the state of a new child. This should only be called
                // on adding a new child. Take into account that the same child
                // can be readded again.
                let child_info = ChildInfo {
                    uuid: NexusChild::uuid(child_uri)
                        .expect("Failed to get child UUID."),
                    healthy: *healthy,
                };

                // Check if there is a child with the same UUID already
                // and update the existing record instead of adding a new one.
                match nexus_info
                    .children
                    .iter()
                    .position(|r| r.uuid == child_info.uuid)
                {
                    Some(idx) => nexus_info.children[idx] = child_info,
                    None => nexus_info.children.push(child_info),
                }
            }
            PersistOp::RemoveChild {
                child_uri,
            } => {
                let uuid = NexusChild::uuid(child_uri)
                    .expect("Failed to get child UUID.");

                nexus_info.children.retain(|child| child.uuid != uuid);
            }
            PersistOp::Update {
                child_uri,
                healthy,
            } => {
                let uuid = NexusChild::uuid(child_uri)
                    .expect("Failed to get child UUID.");
                // Only update the state of the child that has changed. Do not
                // update the other children or "clean shutdown" information.
                // This should only be called on a child state change.
                nexus_info.children.iter_mut().for_each(|c| {
                    if c.uuid == uuid {
                        c.healthy = *healthy;
                    }
                });
            }
            // Only update the state of the child if the precondition holds.
            PersistOp::UpdateCond {
                child_uri,
                healthy,
                ref predicate,
            } => {
                // Do not persist the state if predicate fails.
                if !predicate(nexus_info) {
                    self.set_nexus_io_mode(IoMode::Normal).await;
                    return Ok(());
                }

                let uuid = NexusChild::uuid(child_uri)
                    .expect("Failed to get child UUID.");

                nexus_info.children.iter_mut().for_each(|c| {
                    if c.uuid == uuid {
                        c.healthy = *healthy;
                    }
                });
            }
            PersistOp::Shutdown => {
                // Only update the clean shutdown variable. Do not update the
                // child state information.
                // This should only be called when destroying a nexus.
                nexus_info.clean_shutdown = true;
            }
        }

        match self.save(&persistent_nexus_info).await {
            Ok(_) => {
                self.set_nexus_io_mode(IoMode::Normal).await;
                Ok(())
            }
            Err(e) => {
                // If the operation was an update for shutdown, no need to
                // shutdown in the case of an error.
                if matches!(op, PersistOp::Shutdown) {
                    error!("{self:?}: failed to update persistent store: {e}");
                } else {
                    error!(
                        "{self:?}: failed to update persistent store, \
                        will shutdown the nexus: {e}"
                    );
                    self.try_self_shutdown();
                }
                Err(e)
            }
        }
    }

    // Saves the nexus info to the store. This is integral to ensuring data
    // consistency across restarts of Mayastor. Therefore, keep retrying
    // until successful.
    async fn save(&self, info: &PersistentNexusInfo) -> Result<(), Error> {
        // If a key has been provided, use it to store the NexusInfo; use the
        // nexus uuid as the key otherwise.
        let key = match &info.key {
            Some(k) => k.clone(),
            None => self.uuid().to_string(),
        };

        let mut retry = PersistentStore::retries();
        loop {
            let Err(err) = PersistentStore::put(&key, &info.inner).await else {
                trace!(?key, "{self:?}: the state was saved successfully");
                return Ok(());
            };

            retry -= 1;
            if retry == 0 {
                return Err(Error::SaveStateFailed {
                    source: err,
                    name: self.name.clone(),
                });
            }

            error!(
                "{self:?}: failed to persist nexus information, \
                will retry ({retry} left): {err}"
            );

            // Allow some time for the connection to the persistent
            // store to be re-established before retrying the operation.
            if mayastor_sleep(Duration::from_secs(1)).await.is_err() {
                error!("{self:?}: failed to wait for sleep");
            }
        }
    }
}
