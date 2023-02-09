///! Helpers related to nexus grpc methods.
use mayastor_api::v0 as rpc;
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
            NexusPtpl,
            NexusStatus,
            NvmeAnaState,
            Reason,
        },
        PtplFileOps,
    },
    core::{Protocol, Share},
    rebuild::RebuildJob,
};

/// Map the internal child states into rpc child states (i.e. the states that
/// the control plane sees)
impl From<&NexusChild<'_>> for rpc::ChildState {
    fn from(child: &NexusChild) -> Self {
        if child.is_opened_unsync() {
            return rpc::ChildState::ChildDegraded;
        }

        match child.state() {
            ChildState::Init => rpc::ChildState::ChildDegraded,
            ChildState::ConfigInvalid => rpc::ChildState::ChildFaulted,
            ChildState::Open => rpc::ChildState::ChildOnline,
            ChildState::Destroying => rpc::ChildState::ChildDegraded,
            ChildState::Closed => rpc::ChildState::ChildDegraded,
            ChildState::Faulted(reason) => match reason {
                Reason::NoSpace => rpc::ChildState::ChildDegraded,
                Reason::TimedOut => rpc::ChildState::ChildDegraded,
                Reason::Unknown => rpc::ChildState::ChildFaulted,
                Reason::CantOpen => rpc::ChildState::ChildFaulted,
                Reason::RebuildFailed => rpc::ChildState::ChildFaulted,
                Reason::IoError => rpc::ChildState::ChildFaulted,
                Reason::ByClient => rpc::ChildState::ChildFaulted,
                Reason::AdminCommandFailed => rpc::ChildState::ChildFaulted,
            },
        }
    }
}
impl From<&NexusChild<'_>> for rpc::ChildStateReason {
    fn from(child: &NexusChild) -> Self {
        if child.is_opened_unsync() {
            return Self::OutOfSync;
        }

        match child.state() {
            ChildState::Init => Self::Init,
            ChildState::ConfigInvalid => Self::ConfigInvalid,
            ChildState::Open => Self::None,
            ChildState::Destroying => Self::Closed,
            ChildState::Closed => Self::Closed,
            ChildState::Faulted(reason) => match reason {
                Reason::NoSpace => Self::NoSpace,
                Reason::TimedOut => Self::TimedOut,
                Reason::Unknown => Self::None,
                Reason::CantOpen => Self::CannotOpen,
                Reason::RebuildFailed => Self::RebuildFailed,
                Reason::IoError => Self::IoFailure,
                Reason::ByClient => Self::ByClient,
                Reason::AdminCommandFailed => Self::AdminFailed,
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
            NexusStatus::ShuttingDown => rpc::NexusState::NexusShuttingDown,
            NexusStatus::Shutdown => rpc::NexusState::NexusShutdown,
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
    pub async fn to_grpc(&self) -> rpc::Child {
        rpc::Child {
            uri: self.uri().to_string(),
            state: rpc::ChildState::from(self) as i32,
            rebuild_progress: self.get_rebuild_progress().await,
            reason: rpc::ChildStateReason::from(self) as i32,
            device_name: self.get_device_name(),
        }
    }
}

impl<'n> Nexus<'n> {
    /// Convert nexus object to grpc representation.
    ///
    /// We cannot use From trait because it is not value to value conversion.
    /// All we have is a reference to nexus.
    pub async fn to_grpc(&self) -> rpc::Nexus {
        rpc::Nexus {
            uuid: name_to_uuid(&self.name).to_string(),
            size: self.req_size(),
            state: rpc::NexusState::from(self.status()) as i32,
            device_uri: self.get_share_uri().unwrap_or_default(),
            children: {
                let mut children = Vec::with_capacity(self.children().len());
                for child in self.children_iter() {
                    children.push(child.to_grpc().await)
                }
                children
            },
            rebuilds: RebuildJob::count() as u32,
            allowed_hosts: self.allowed_hosts(),
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
            size: self.req_size(),
            state: rpc::NexusState::from(self.status()) as i32,
            device_uri: self.get_share_uri().unwrap_or_default(),
            children: {
                let mut children = Vec::with_capacity(self.children().len());
                for child in self.children_iter() {
                    children.push(child.to_grpc().await)
                }
                children
            },
            rebuilds: RebuildJob::count() as u32,
            ana_state: ana_state as i32,
            allowed_hosts: self.allowed_hosts(),
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
    match n.child_mut(&args.uri) {
        Ok(child) => Ok(child.to_grpc().await),
        Err(error) => Err(error),
    }
}

/// Idempotent destruction of the nexus.
pub async fn nexus_destroy(uuid: &str) -> Result<(), nexus::Error> {
    if let Ok(n) = nexus_lookup(uuid) {
        let result = n.destroy().await;
        if result.is_ok() {
            info!("Destroyed nexus: '{}'", uuid);
        } else {
            return result;
        }
    } else if let Ok(uuid) = Uuid::parse_str(uuid) {
        NexusPtpl::new(uuid).destroy().ok();
    }

    Ok(())
}
