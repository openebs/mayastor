use super::{ChildState, Nexus, NexusChild};
use crate::{persistent_store::PersistentStore, sleep::mayastor_sleep};
use serde::{Deserialize, Serialize};
use std::time::Duration;

type ChildUri = String;

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
    AddChild((ChildUri, ChildState)),
    /// Update a persistent entry.
    Update((ChildUri, ChildState)),
    /// Update a persistent entry only when a precondition on this NexusInfo
    /// holds. Predicate is called under protection of the NexusInfo lock,
    /// so the check is assumed to be atomic and not interfering with other
    /// modifications of the same NexusInfo.
    UpdateCond((ChildUri, ChildState, &'a dyn Fn(&NexusInfo) -> bool)),
    /// Save the clean shutdown variable.
    Shutdown,
}

impl<'n> Nexus<'n> {
    /// Persist information to the store.
    pub(crate) async fn persist(&self, op: PersistOp<'_>) {
        if !PersistentStore::enabled() {
            return;
        }

        let mut nexus_info = self.nexus_info.lock().await;
        match op {
            PersistOp::Create => {
                // Initialisation of the persistent info will overwrite any
                // existing entries.
                // This should only be called on nexus creation, therefore we
                // expect the NexusInfo structure to contain default values.
                assert!(nexus_info.children.is_empty());
                assert!(!nexus_info.clean_shutdown);
                self.children.iter().for_each(|c| {
                    let child_info = ChildInfo {
                        uuid: NexusChild::uuid(&c.name)
                            .expect("Failed to get child UUID."),
                        healthy: Self::child_healthy(&c.state()),
                    };
                    nexus_info.children.push(child_info);
                });
            }
            PersistOp::AddChild((uri, state)) => {
                // Add the state of a new child.
                // This should only be called on adding a new child.
                let child_info = ChildInfo {
                    uuid: NexusChild::uuid(&uri)
                        .expect("Failed to get child UUID."),
                    healthy: Self::child_healthy(&state),
                };
                nexus_info.children.push(child_info);
            }
            PersistOp::Update((uri, state)) => {
                let uuid =
                    NexusChild::uuid(&uri).expect("Failed to get child UUID.");
                // Only update the state of the child that has changed. Do not
                // update the other children or "clean shutdown" information.
                // This should only be called on a child state change.
                nexus_info.children.iter_mut().for_each(|c| {
                    if c.uuid == uuid {
                        c.healthy = Self::child_healthy(&state);
                    }
                });
            }
            // Only update the state of the child if the precondition holds.
            PersistOp::UpdateCond((uri, state, f)) => {
                // Do not persist the state if predicate fails.
                if !f(&nexus_info) {
                    return;
                }

                let uuid =
                    NexusChild::uuid(&uri).expect("Failed to get child UUID.");

                nexus_info.children.iter_mut().for_each(|c| {
                    if c.uuid == uuid {
                        c.healthy = Self::child_healthy(&state);
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
        self.save(&nexus_info).await;
    }

    /// Determine child health.
    fn child_healthy(state: &ChildState) -> bool {
        state == &ChildState::Open
    }

    // Save the nexus info to the store. This is integral to ensuring data
    // consistency across restarts of Mayastor. Therefore, keep retrying
    // until successful.
    // TODO: Should we give up retrying eventually?
    async fn save(&self, info: &NexusInfo) {
        let mut output_err = true;
        let nexus_uuid = self.uuid().to_string();
        loop {
            match PersistentStore::put(&nexus_uuid, info).await {
                Ok(_) => {
                    // The state was saved successfully.
                    break;
                }
                Err(e) => {
                    // Output an error message on first failure. Thereafter
                    // silently retry.
                    if output_err {
                        error!(
                            "Failed to persist nexus information for nexus {}, UUID {} with error {}. Retrying...",
                            self.name,
                            nexus_uuid,
                            e
                        );
                        output_err = false;
                    }

                    // Allow some time for the connection to the persistent
                    // store to be re-established before retrying the operation.
                    let rx = mayastor_sleep(Duration::from_secs(1));
                    if rx.await.is_err() {
                        // Failed to wait for sleep but just carry on around the
                        // loop and try the 'put' again anyway.
                        error!("Failed to wait for Mayastor sleep");
                    }
                }
            }
        }
    }
}
