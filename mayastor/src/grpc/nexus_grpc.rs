//! Helpers related to nexus grpc methods.

use rpc::mayastor as rpc;
use std::convert::From;
use uuid::Uuid;

use crate::{
    bdev::nexus::{
        instances,
        nexus_bdev::{Error, Nexus, NexusStatus},
        nexus_child::{ChildStatus, NexusChild},
    },
    rebuild::RebuildJob,
};

impl From<ChildStatus> for rpc::ChildState {
    fn from(child: ChildStatus) -> Self {
        match child {
            ChildStatus::Faulted => rpc::ChildState::ChildFaulted,
            ChildStatus::Degraded => rpc::ChildState::ChildDegraded,
            ChildStatus::Online => rpc::ChildState::ChildOnline,
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

impl NexusChild {
    /// Convert nexus child object to grpc representation.
    ///
    /// We cannot use From trait because it is not value to value conversion.
    /// All we have is a reference to a child.
    pub fn to_grpc(&self) -> rpc::Child {
        rpc::Child {
            uri: self.name.clone(),
            state: rpc::ChildState::from(self.status()) as i32,
            rebuild_progress: self.get_rebuild_progress(),
        }
    }
}

impl Nexus {
    /// Convert nexus object to grpc representation.
    ///
    /// We cannot use From trait because it is not value to value conversion.
    /// All we have is a reference to nexus.
    pub fn to_grpc(&self) -> rpc::Nexus {
        rpc::Nexus {
            uuid: name_to_uuid(&self.name).to_string(),
            size: self.size,
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
}

/// Convert nexus name to uuid.
///
/// This function never fails which means that if there is a nexus with
/// unconventional name that likely means it was not created using nexus
/// rpc api, we return the whole name without modifications as it is.
fn name_to_uuid(name: &str) -> &str {
    if name.starts_with("nexus-") {
        &name[6 ..]
    } else {
        name
    }
}

/// Convert the UUID to a nexus name in the form of "nexus-{uuid}".
/// Return error if the UUID is not valid.
pub fn uuid_to_name(uuid: &str) -> Result<String, Error> {
    match Uuid::parse_str(uuid) {
        Ok(uuid) => Ok(format!("nexus-{}", uuid.to_hyphenated().to_string())),
        Err(_) => Err(Error::InvalidUuid {
            uuid: uuid.to_owned(),
        }),
    }
}

/// Lookup a nexus by its uuid prepending "nexus-" prefix. Return error if
/// uuid is invalid or nexus not found.
pub fn nexus_lookup(uuid: &str) -> Result<&mut Nexus, Error> {
    let name = uuid_to_name(uuid)?;

    if let Some(nexus) = instances().iter_mut().find(|n| n.name == name) {
        Ok(nexus)
    } else {
        Err(Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

/// Add child to nexus. Normally this would have been part of grpc method
/// implementation, however it is not allowed to use '?' in `locally` macro.
/// So we implement it as a separate function.
pub async fn nexus_add_child(
    args: rpc::AddChildNexusRequest,
) -> Result<rpc::Child, Error> {
    let n = nexus_lookup(&args.uuid)?;
    n.add_child(&args.uri, args.norebuild).await?;
    n.get_child_by_name(&args.uri).map(|ch| ch.to_grpc())
}
