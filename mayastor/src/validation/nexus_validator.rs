//! Module responsible for validating that an operation can be performed on a
//! nexus.

use crate::bdev::{
    nexus::{
        nexus_bdev::{
            Error,
            Error::{NexusNotFound, ValidationFailed},
        },
        nexus_child::{NexusChild, StatusReasons},
        nexus_child_status_config::ChildStatusConfig,
    },
    nexus_lookup,
    ChildStatus,
    Nexus,
    NexusStatus,
};
use snafu::Snafu;
use std::future::Future;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum NexusValidationError {
    #[snafu(display("The nexus {} is in the faulted state", name))]
    Faulted { name: String },
    #[snafu(display("The nexus {} does not have any healthy children", name))]
    NoHealthyChild { name: String },
    #[snafu(display(
        "Child {} of nexus {} is the only healthy child",
        child_name,
        nexus_name
    ))]
    LastHealthyChild {
        child_name: String,
        nexus_name: String,
    },
    #[snafu(display("Failed to update the child status configuration"))]
    FailedUpdateChildStatusConfig,
}

/// Perform add child validation. If this passes, execute the future.
pub(crate) async fn add_child<F>(
    nexus_name: &str,
    child_name: &str,
    future: F,
) -> Result<NexusStatus, Error>
where
    F: Future<Output = Result<NexusStatus, Error>>,
{
    let nexus = nexus_exists(nexus_name)?;
    if is_nexus_faulted(&nexus) {
        return Err(ValidationFailed {
            source: NexusValidationError::Faulted {
                name: nexus.name.clone(),
            },
        });
    }

    if !has_healthy_child(&nexus) {
        return Err(ValidationFailed {
            source: NexusValidationError::NoHealthyChild {
                name: nexus.name.clone(),
            },
        });
    }

    // Add the child to the status configuration as out-of-sync before executing
    // the future. The status configuration will be updated automatically
    // when the newly added child comes online.
    let mut status_reasons = StatusReasons::new();
    status_reasons.out_of_sync(true);
    if ChildStatusConfig::add(child_name, &status_reasons).is_err() {
        return Err(ValidationFailed {
            source: NexusValidationError::FailedUpdateChildStatusConfig {},
        });
    }

    // Validation has passed, execute the "add child" future.
    match future.await {
        Ok(value) => Ok(value),
        Err(e) => {
            rollback_child_status_config();
            Err(e)
        }
    }
}

/// Perform remove child validation. If this passes, execute the future.
pub(crate) async fn remove_child<F>(
    nexus_name: &str,
    child_name: &str,
    future: F,
) -> Result<(), Error>
where
    F: Future<Output = Result<(), Error>>,
{
    let nexus = nexus_exists(nexus_name)?;
    if nexus.child_count == 1 {
        return Err(Error::DestroyLastChild {
            name: nexus_name.to_string(),
            child: child_name.to_string(),
        });
    }

    if is_last_healthy_child(nexus, child_name) {
        return Err(ValidationFailed {
            source: NexusValidationError::LastHealthyChild {
                child_name: child_name.to_string(),
                nexus_name: nexus_name.to_string(),
            },
        });
    }

    // Remove the child from the configuration before executing the future
    if ChildStatusConfig::remove(child_name).is_err() {
        return Err(ValidationFailed {
            source: NexusValidationError::FailedUpdateChildStatusConfig {},
        });
    }

    // Validation has passed, execute the "remove child" future.
    match future.await {
        Ok(_) => Ok(()),
        Err(e) => {
            rollback_child_status_config();
            Err(e)
        }
    }
}

// Checks if a given nexus exists
fn nexus_exists(nexus_name: &str) -> Result<&Nexus, Error> {
    match nexus_lookup(nexus_name) {
        Some(nexus) => Ok(nexus),
        None => Err(NexusNotFound {
            name: nexus_name.to_string(),
        }),
    }
}

/// Checks if a nexus is in the faulted state
fn is_nexus_faulted(nexus: &Nexus) -> bool {
    nexus.status() == NexusStatus::Faulted
}

/// Checks if a nexus has at least one healthy child
fn has_healthy_child(nexus: &Nexus) -> bool {
    nexus
        .children
        .iter()
        .any(|c| c.status() == ChildStatus::Online)
}

// Checks if the child is the last remaining healthy child
fn is_last_healthy_child(nexus: &Nexus, child_name: &str) -> bool {
    let healthy_children = nexus
        .children
        .iter()
        .filter(|child| child.status() == ChildStatus::Online)
        .collect::<Vec<&NexusChild>>();

    healthy_children.len() == 1 && healthy_children[0].name == child_name
}

/// Attempt to rollback changes to the status configuration. This is called when
/// an operation fails. The best we can do here is to save the current status of
/// the running system as it is not known at what point the operation failed.
fn rollback_child_status_config() {
    // TODO: Determine what to do in this case. If the operation itself has
    // succeeded the running system is still good.
    if ChildStatusConfig::save().is_err() {
        error!("Failed to save child status configuration");
    }
}
