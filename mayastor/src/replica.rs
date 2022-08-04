//! The high-level replica object methods.
//!
//! Replica is a logical data volume exported over nvmf (in SPDK terminology
//! an lvol). Here we define methods for easy management of replicas.
#![allow(dead_code)]
use std::{ffi::CStr, os::raw::c_char};

use ::rpc::mayastor as rpc;
use snafu::{ResultExt, Snafu};

use spdk_rs::libspdk::{spdk_lvol, vbdev_lvol_get_from_bdev};

use crate::{
    core::{Bdev, UntypedBdev},
    subsys::NvmfError,
    target,
};

/// These are high-level context errors one for each rpc method.
#[derive(Debug, Snafu)]
pub enum RpcError {
    #[snafu(display("Failed to (un)share replica {}", uuid))]
    ShareReplica { source: Error, uuid: String },
}

impl From<RpcError> for tonic::Status {
    fn from(e: RpcError) -> Self {
        match e {
            RpcError::ShareReplica {
                source, ..
            } => Self::from(source),
        }
    }
}

// Replica errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Replica has been already shared"))]
    ReplicaShared {},
    #[snafu(display("share nvmf"))]
    ShareNvmf { source: NvmfError },
    #[snafu(display("unshare nvmf"))]
    UnshareNvmf { source: NvmfError },
    #[snafu(display("Invalid share protocol {} in request", protocol))]
    InvalidProtocol { protocol: i32 },
    #[snafu(display("Replica does not exist"))]
    ReplicaNotFound {},
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> Self {
        match error {
            Error::InvalidProtocol {
                ..
            } => Self::invalid_argument(error.to_string()),
            Error::ReplicaNotFound {} => Self::not_found(error.to_string()),
            _ => Self::internal(error.to_string()),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Structure representing a replica which is basically SPDK lvol.
///
/// Note about safety: The structure wraps raw C pointer from SPDK.
/// It is safe to use only in synchronous context. If you keep Replica for
/// longer than that then something else can run on reactor_0 inbetween
/// which may destroy the replica and invalidate the pointer!
pub struct Replica {
    lvol_ptr: *mut spdk_lvol,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
/// Types of remote access storage protocols and IDs for sharing replicas.
pub enum ShareType {
    Nvmf,
}

/// Detect share protocol (if any) for replica with given uuid and share ID
/// string.
fn detect_share(uuid: &str) -> Option<(ShareType, String)> {
    // first try nvmf ...
    if let Some(uri) = target::nvmf::get_uri(uuid) {
        return Some((ShareType::Nvmf, uri));
    }

    None
}

impl Replica {
    /// Lookup replica by uuid (=name).
    pub fn lookup(uuid: &str) -> Option<Self> {
        match UntypedBdev::lookup_by_name(uuid) {
            Some(bdev) => Replica::from_bdev(bdev),
            None => None,
        }
    }

    /// Look up replica from Bdev
    pub fn from_bdev(mut bdev: UntypedBdev) -> Option<Self> {
        let lvol =
            unsafe { vbdev_lvol_get_from_bdev(bdev.unsafe_inner_mut_ptr()) };
        if lvol.is_null() {
            None
        } else {
            Some(Self {
                lvol_ptr: lvol,
            })
        }
    }

    /// Expose replica over supported remote access storage protocols (nvmf).
    pub async fn share(&self, kind: ShareType) -> Result<()> {
        let name = self.get_name().to_owned();
        if detect_share(&name).is_some() {
            return Err(Error::ReplicaShared {});
        }

        let bdev = unsafe {
            UntypedBdev::checked_from_ptr((*self.lvol_ptr).bdev).unwrap()
        };

        match kind {
            ShareType::Nvmf => target::nvmf::share(&name, &bdev)
                .await
                .context(ShareNvmf {})?,
        }
        Ok(())
    }

    /// The opposite of share. It is not an error to call unshare on a replica
    /// which is not shared.
    pub async fn unshare(&self) -> Result<()> {
        let name = self.get_name().to_owned();
        if let Some((share_type, _)) = detect_share(&name) {
            match share_type {
                ShareType::Nvmf => target::nvmf::unshare(&name)
                    .await
                    .context(UnshareNvmf {})?,
            }
        };
        Ok(())
    }

    /// Return either a type of share and a string identifying the share
    /// (nqn for nvmf) or none if the replica is not
    /// shared.
    pub fn get_share_type(&self) -> Option<ShareType> {
        detect_share(self.get_name()).map(|val| val.0)
    }

    /// Return storage URI understood & used by nexus to access the replica.
    pub fn get_share_uri(&self) -> String {
        let uri_no_uuid = match detect_share(self.get_name()) {
            Some((_, share_uri)) => share_uri,
            None => {
                format!("bdev:///{}", self.get_name())
            }
        };
        format!("{}?uuid={}", uri_no_uuid, self.get_uuid())
    }

    /// Get size of the replica in bytes.
    pub fn get_size(&self) -> u64 {
        unsafe {
            UntypedBdev::checked_from_ptr((*self.lvol_ptr).bdev)
                .unwrap()
                .size_in_bytes()
        }
    }

    /// Get name of the pool which replica belongs to.
    pub fn get_pool_name(&self) -> &str {
        unsafe {
            let lvs = &*(*self.lvol_ptr).lvol_store;
            CStr::from_ptr(&lvs.name as *const c_char).to_str().unwrap()
        }
    }

    /// Get name of the replica.
    pub fn get_name(&self) -> &str {
        unsafe {
            CStr::from_ptr(&(*self.lvol_ptr).name as *const c_char)
                .to_str()
                .unwrap()
        }
    }

    /// Get uuid of the replica.
    pub fn get_uuid(&self) -> &str {
        unsafe {
            CStr::from_ptr(&(*self.lvol_ptr).uuid_str as *const c_char)
                .to_str()
                .unwrap()
        }
    }

    /// Return if replica has been thin provisioned.
    pub fn is_thin(&self) -> bool {
        unsafe { (*self.lvol_ptr).thin_provision }
    }
}

/// Iterator over replicas
#[derive(Default)]
pub struct ReplicaIter {
    /// Last bdev examined by the iterator during the call to next()
    bdev: Option<UntypedBdev>,
}

impl ReplicaIter {
    pub fn new() -> ReplicaIter {
        ReplicaIter {
            bdev: None,
        }
    }
}

// XXX: shut this not simply be a bdev iterator with .filter()?

impl Iterator for ReplicaIter {
    type Item = Replica;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_bdev = match &mut self.bdev {
                Some(bdev) => unsafe {
                    let ptr = spdk_rs::libspdk::spdk_bdev_next(
                        bdev.unsafe_inner_mut_ptr(),
                    );
                    Bdev::checked_from_ptr(ptr)
                },
                None => UntypedBdev::bdev_first(),
            };

            let mut bdev = match maybe_bdev {
                Some(bdev) => bdev,
                None => return None,
            };

            // Skip all other bdevs which are not lvols (i.e. aio)
            let lvol = unsafe {
                vbdev_lvol_get_from_bdev(bdev.unsafe_inner_mut_ptr())
            };
            if !lvol.is_null() {
                let mut aliases = bdev.aliases();
                // each lvol has a first alias of form "pool/lvol-name"
                if !aliases.is_empty() {
                    let alias = aliases.remove(0);
                    let parts: Vec<&str> = alias.split('/').collect();

                    if parts.len() == 2 && bdev.name() == parts[1] {
                        let replica = Replica {
                            lvol_ptr: lvol,
                        };

                        if replica.get_pool_name() == parts[0] {
                            // we found a replica
                            self.bdev = Some(bdev);
                            return Some(replica);
                        }
                    }
                }
            }
            self.bdev = Some(bdev);
        }
    }
}

impl From<Replica> for rpc::Replica {
    fn from(r: Replica) -> Self {
        rpc::Replica {
            // we currently report the replica name as the volume uuid
            uuid: r.get_name().to_owned(),
            pool: r.get_pool_name().to_owned(),
            size: r.get_size(),
            thin: r.is_thin(),
            share: match r.get_share_type() {
                Some(share_type) => match share_type {
                    ShareType::Nvmf => rpc::ShareProtocolReplica::ReplicaNvmf,
                },
                None => rpc::ShareProtocolReplica::ReplicaNone,
            } as i32,
            uri: r.get_share_uri(),
        }
    }
}

pub(crate) async fn share_replica(
    args: rpc::ShareReplicaRequest,
) -> Result<rpc::ShareReplicaReply, RpcError> {
    let want_share = match rpc::ShareProtocolReplica::from_i32(args.share) {
        Some(val) => val,
        None => Err(Error::InvalidProtocol {
            protocol: args.share,
        })
        .context(ShareReplica {
            uuid: args.uuid.clone(),
        })?,
    };
    let replica = match Replica::lookup(&args.uuid) {
        Some(replica) => replica,
        None => Err(Error::ReplicaNotFound {}).context(ShareReplica {
            uuid: args.uuid.clone(),
        })?,
    };
    // first unshare the replica if there is a protocol change
    let unshare = match replica.get_share_type() {
        Some(share_type) => match share_type {
            ShareType::Nvmf => {
                want_share != rpc::ShareProtocolReplica::ReplicaNvmf
            }
        },
        None => false,
    };
    if unshare {
        replica.unshare().await.context(ShareReplica {
            uuid: args.uuid.clone(),
        })?;
    }
    // share the replica if it is not shared, and we want it to be
    // shared
    if replica.get_share_type().is_none() {
        match want_share {
            rpc::ShareProtocolReplica::ReplicaNvmf => {
                replica.share(ShareType::Nvmf).await.context(ShareReplica {
                    uuid: args.uuid.clone(),
                })?
            }
            rpc::ShareProtocolReplica::ReplicaIscsi
            | rpc::ShareProtocolReplica::ReplicaNone => (),
        }
    }
    Ok(rpc::ShareReplicaReply {
        uri: replica.get_share_uri(),
    })
}
