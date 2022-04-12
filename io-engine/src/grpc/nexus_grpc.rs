///! Helpers related to nexus grpc methods.
use rpc::mayastor as rpc;
use std::{convert::From, pin::Pin};
use uuid::Uuid;

use crate::{
    bdev::{
        nexus,
        nexus::{
            nexus_lookup_mut,
            nexus_lookup_uuid_mut,
            ChildState,
            Nexus,
            NexusChild,
            NexusStatus,
            NvmeAnaState,
            Reason,
        },
    },
    core::{Protocol, Share},
    rebuild::RebuildJob,
};

/// Map the internal child states into rpc child states (i.e. the states that
/// the control plane sees)
impl From<ChildState> for rpc::ChildState {
    fn from(child: ChildState) -> Self {
        match child {
            ChildState::Init => rpc::ChildState::ChildDegraded,
            ChildState::ConfigInvalid => rpc::ChildState::ChildFaulted,
            ChildState::Open => rpc::ChildState::ChildOnline,
            ChildState::Destroying => rpc::ChildState::ChildDegraded,
            ChildState::Closed => rpc::ChildState::ChildDegraded,
            ChildState::Faulted(reason) => match reason {
                Reason::OutOfSync => rpc::ChildState::ChildDegraded,
                _ => rpc::ChildState::ChildFaulted,
            },
        }
    }
}
impl From<NexusStatus> for rpc::NexusState {
    fn from(nexus: NexusStatus) -> Self {
        match nexus {
            NexusStatus::Faulted => rpc::NexusState::NexusFaulted,
            NexusStatus::Degraded => rpc::NexusState::NexusDegraded,
            NexusStatus::Online => rpc::NexusState::NexusOnline,
        }
    }
}

impl From<NvmeAnaState> for rpc::NvmeAnaState {
    fn from(state: NvmeAnaState) -> Self {
        match state {
            NvmeAnaState::InvalidState => {
                rpc::NvmeAnaState::NvmeAnaInvalidState
            }
            NvmeAnaState::OptimizedState => {
                rpc::NvmeAnaState::NvmeAnaOptimizedState
            }
            NvmeAnaState::NonOptimizedState => {
                rpc::NvmeAnaState::NvmeAnaNonOptimizedState
            }
            NvmeAnaState::InaccessibleState => {
                rpc::NvmeAnaState::NvmeAnaInaccessibleState
            }
            NvmeAnaState::PersistentLossState => {
                rpc::NvmeAnaState::NvmeAnaPersistentLossState
            }
            NvmeAnaState::ChangeState => rpc::NvmeAnaState::NvmeAnaChangeState,
        }
    }
}

impl<'c> NexusChild<'c> {
    /// Convert nexus child object to grpc representation.
    ///
    /// We cannot use From trait because it is not value to value conversion.
    /// All we have is a reference to a child.
    pub fn to_grpc(&self) -> rpc::Child {
        rpc::Child {
            uri: self.get_name().to_string(),
            state: rpc::ChildState::from(self.state()) as i32,
            rebuild_progress: self.get_rebuild_progress(),
        }
    }
}

impl<'n> Nexus<'n> {
    /// Convert nexus object to grpc representation.
    ///
    /// We cannot use From trait because it is not value to value conversion.
    /// All we have is a reference to nexus.
    pub fn to_grpc(&self) -> rpc::Nexus {
        rpc::Nexus {
            uuid: name_to_uuid(&self.name).to_string(),
            size: self.req_size,
            state: rpc::NexusState::from(self.status()) as i32,
            device_uri: self.get_share_uri().unwrap_or_default(),
            children: self
                .children
                .iter()
                .map(|ch| ch.to_grpc())
                .collect::<Vec<_>>(),
            rebuilds: RebuildJob::count() as u32,
        }
    }

    pub async fn to_grpc_v2(&self) -> rpc::NexusV2 {
        let mut ana_state = rpc::NvmeAnaState::NvmeAnaInvalidState;

        // Get ANA state only for published nexuses.
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Ok(state) = self.get_ana_state().await {
                ana_state = rpc::NvmeAnaState::from(state);
            }
        }

        rpc::NexusV2 {
            name: name_to_uuid(&self.name).to_string(),
            uuid: self.uuid().to_string(),
            size: self.req_size,
            state: rpc::NexusState::from(self.status()) as i32,
            device_uri: self.get_share_uri().unwrap_or_default(),
            children: self
                .children
                .iter()
                .map(|ch| ch.to_grpc())
                .collect::<Vec<_>>(),
            rebuilds: RebuildJob::count() as u32,
            ana_state: ana_state as i32,
        }
    }
}

/// Convert nexus name to uuid.
///
/// This function never fails which means that if there is a nexus with
/// unconventional name that likely means it was not created using nexus
/// rpc api, we return the whole name without modifications as it is.
fn name_to_uuid(name: &str) -> &str {
    if let Some(stripped) = name.strip_prefix("nexus-") {
        stripped
    } else {
        name
    }
}

/// Convert the UUID to a nexus name in the form of "nexus-{uuid}".
/// Return error if the UUID is not valid.
pub fn uuid_to_name(uuid: &str) -> Result<String, nexus::Error> {
    match Uuid::parse_str(uuid) {
        Ok(uuid) => Ok(format!("nexus-{}", uuid.to_hyphenated())),
        Err(_) => Err(nexus::Error::InvalidUuid {
            uuid: uuid.to_owned(),
        }),
    }
}

/// Look up a nexus by name first (if created by nexus_create_v2) then by its
/// uuid prepending "nexus-" prefix.
/// Return error if nexus not found.
pub fn nexus_lookup<'n>(
    uuid: &str,
) -> Result<Pin<&'n mut Nexus<'n>>, nexus::Error> {
    if let Some(nexus) = nexus_lookup_mut(uuid) {
        Ok(nexus)
    } else if let Some(nexus) = nexus_lookup_uuid_mut(uuid) {
        Ok(nexus)
    } else {
        let name = uuid_to_name(uuid)?;
        if let Some(nexus) = nexus_lookup_mut(&name) {
            Ok(nexus)
        } else {
            Err(nexus::Error::NexusNotFound {
                name: uuid.to_owned(),
            })
        }
    }
}

/// Add child to nexus. Normally this would have been part of grpc method
/// implementation, however it is not allowed to use '?' in `locally` macro.
/// So we implement it as a separate function.
pub async fn nexus_add_child(
    args: rpc::AddChildNexusRequest,
) -> Result<rpc::Child, nexus::Error> {
    let mut n = nexus_lookup(&args.uuid)?;
    // TODO: do not add child if it already exists (idempotency)
    // For that we need api to check existence of child by name (not uri that
    // contain parameters that may change).
    n.as_mut().add_child(&args.uri, args.norebuild).await?;
    n.get_child_by_name(&args.uri).map(|ch| ch.to_grpc())
}

/// Idempotent destruction of the nexus.
pub async fn nexus_destroy(uuid: &str) -> Result<(), nexus::Error> {
    if let Ok(n) = nexus_lookup(uuid) {
        let result = n.destroy().await;
        if result.is_ok() {
            info!("Nexus {} destroyed", uuid)
        } else {
            return result;
        }
    };
    Ok(())
}
