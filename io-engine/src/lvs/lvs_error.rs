use nix::errno::Errno;
use snafu::Snafu;

use super::PropName;

use crate::{
    bdev_api::BdevError,
    core::{CoreError, ToErrno},
};

/// LVS import error reason.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum ImportErrorReason {
    #[snafu(display(""))]
    None,
    #[snafu(display(": existing pool disk has different name: {name}"))]
    NameMismatch { name: String },
    #[snafu(display(": another pool already exists with this name: {name}"))]
    NameClash { name: String },
    #[snafu(display(": existing pool has different uuid: {uuid}"))]
    UuidMismatch { uuid: String },
}

/// Low-level blob store errors.
/// This error type is introduced to eliminate the use of low-level `Errno`
/// codes in high-level LVS code.
#[derive(Debug, Snafu, Copy, Clone)]
pub enum BsError {
    #[snafu(display(""))]
    Generic { source: Errno },
    #[snafu(display(""))]
    InvalidArgument {},
    #[snafu(display(": volume not found"))]
    LvolNotFound {},
    #[snafu(display(": volume already exists"))]
    VolAlreadyExists {},
    #[snafu(display(": volume is busy"))]
    VolBusy {},
    #[snafu(display(": cannot import LVS"))]
    CannotImportLvs {},
    #[snafu(display(": LVS not found or was not loaded"))]
    LvsNotFound {},
    #[snafu(display(": LVS name or UUID mismatch"))]
    LvsIdMismatch {},
    #[snafu(display(": not enough space"))]
    NoSpace {},
    #[snafu(display(": out of metadata pages"))]
    OutOfMetadata {},
    #[snafu(display(": capacity overflow"))]
    CapacityOverflow {},
}

impl BsError {
    /// Creates a `BsError` from an `Errno` value.
    pub fn from_errno(value: Errno) -> Self {
        match value {
            Errno::UnknownErrno => {
                // Unknown errno may indicate that the source negative i32 value
                // was passed instead of taking the abs.
                warn!("Blob store: got unknown errno");
                BsError::Generic {
                    source: value,
                }
            }
            Errno::EINVAL => BsError::InvalidArgument {},
            Errno::ENOENT => BsError::LvolNotFound {},
            Errno::EEXIST => BsError::VolAlreadyExists {},
            Errno::EBUSY => BsError::VolBusy {},
            Errno::EILSEQ => BsError::CannotImportLvs {},
            Errno::ENOMEDIUM => BsError::LvsNotFound {},
            Errno::EMEDIUMTYPE => BsError::LvsIdMismatch {},
            Errno::ENOSPC => BsError::NoSpace {},
            Errno::EMFILE => BsError::OutOfMetadata {},
            Errno::EOVERFLOW => BsError::CapacityOverflow {},
            _ => BsError::Generic {
                source: value,
            },
        }
    }

    /// Creates a `BsError` from a raw i32 errno value.
    pub fn from_i32(value: i32) -> Self {
        let r = Errno::from_i32(value.abs());

        if value < 0 {
            warn!("Blob store: negative errno passed: {r}");
        }

        Self::from_errno(r)
    }
}

impl ToErrno for BsError {
    fn to_errno(self) -> Errno {
        match self {
            Self::Generic {
                source,
            } => source,
            Self::InvalidArgument {} => Errno::EINVAL,
            Self::LvolNotFound {} => Errno::ENOENT,
            Self::VolAlreadyExists {} => Errno::EEXIST,
            Self::VolBusy {} => Errno::EBUSY,
            Self::CannotImportLvs {} => Errno::EILSEQ,
            Self::LvsNotFound {} => Errno::ENOMEDIUM,
            Self::LvsIdMismatch {} => Errno::EMEDIUMTYPE,
            Self::NoSpace {} => Errno::ENOSPC,
            Self::OutOfMetadata {} => Errno::EMFILE,
            Self::CapacityOverflow {} => Errno::EOVERFLOW,
        }
    }
}

/// LVS errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum LvsError {
    #[snafu(display("{source}, failed to import pool {name}{reason}"))]
    Import {
        source: BsError,
        name: String,
        reason: ImportErrorReason,
    },
    #[snafu(display("{source}, failed to create pool {name}"))]
    PoolCreate {
        source: BsError,
        name: String,
    },
    #[snafu(display("{source}, failed to export pool {name}"))]
    Export {
        source: BsError,
        name: String,
    },
    #[snafu(display("{source}, failed to destroy pool {name}"))]
    Destroy {
        source: BdevError,
        name: String,
    },
    #[snafu(display("{source}, failed to grow pool {name}"))]
    Grow {
        source: BsError,
        name: String,
    },
    #[snafu(display("{}", msg))]
    PoolNotFound {
        source: BsError,
        msg: String,
    },
    InvalidBdev {
        source: BdevError,
        name: String,
    },
    #[snafu(display("errno {}: {}", source, msg))]
    Invalid {
        source: BsError,
        msg: String,
    },
    #[snafu(display(
        "errno {}: Invalid cluster-size {}, for pool {}",
        source,
        msg,
        name
    ))]
    InvalidClusterSize {
        source: BsError,
        name: String,
        msg: String,
    },
    #[snafu(display("pool {name}: invalid metadata parameter: {msg}"))]
    InvalidMetadataParam {
        name: String,
        msg: String,
    },
    #[snafu(display("lvol exists {}", name))]
    RepExists {
        source: BsError,
        name: String,
    },
    #[snafu(display("errno: {} failed to create lvol {}", source, name))]
    RepCreate {
        source: BsError,
        name: String,
    },
    #[snafu(display("failed to destroy lvol {} {}", name, if msg.is_empty() { "" } else { msg.as_str() }))]
    RepDestroy {
        source: BsError,
        name: String,
        msg: String,
    },
    #[snafu(display("failed to resize lvol {}", name))]
    RepResize {
        source: BsError,
        name: String,
    },
    #[snafu(display("bdev {} is not a lvol", name))]
    NotALvol {
        source: BsError,
        name: String,
    },
    #[snafu(display("failed to share lvol {}", name))]
    LvolShare {
        source: CoreError,
        name: String,
    },
    #[snafu(display("failed to update share properties lvol {}", name))]
    UpdateShareProperties {
        source: CoreError,
        name: String,
    },
    #[snafu(display("failed to unshare lvol {}", name))]
    LvolUnShare {
        source: CoreError,
        name: String,
    },
    #[snafu(display(
        "failed to get property {} ({}) from {}",
        prop,
        source,
        name
    ))]
    GetProperty {
        source: BsError,
        prop: PropName,
        name: String,
    },
    #[snafu(display("failed to set property {} on {}", prop, name))]
    SetProperty {
        source: BsError,
        prop: String,
        name: String,
    },
    #[snafu(display("failed to sync properties {}", name))]
    SyncProperty {
        source: BsError,
        name: String,
    },
    #[snafu(display("invalid property value: {}", name))]
    Property {
        source: BsError,
        name: String,
    },
    #[snafu(display("invalid replica share protocol value: {}", value))]
    ReplicaShareProtocol {
        value: i32,
    },
    #[snafu(display("Snapshot {} creation failed", msg))]
    SnapshotCreate {
        source: BsError,
        msg: String,
    },
    #[snafu(display("SnapshotClone {} creation failed", msg))]
    SnapshotCloneCreate {
        source: BsError,
        msg: String,
    },
    #[snafu(display("Flush Failed for replica {}", name))]
    FlushFailed {
        name: String,
    },
    #[snafu(display(
        "Snapshot parameters for replica {} is not correct: {}",
        name,
        msg
    ))]
    SnapshotConfigFailed {
        name: String,
        msg: String,
    },
    #[snafu(display(
        "Clone parameters for replica {} are not correct: {}",
        name,
        msg
    ))]
    CloneConfigFailed {
        name: String,
        msg: String,
    },
    #[snafu(display("Failed to wipe the replica"))]
    WipeFailed {
        source: crate::core::wiper::Error,
    },
    #[snafu(display("Failed to acquire resource lock, {}", msg))]
    ResourceLockFailed {
        msg: String,
    },
}

/// Map CoreError to errno code.
impl ToErrno for LvsError {
    fn to_errno(self) -> Errno {
        match self {
            Self::Import {
                source, ..
            } => source.to_errno(),
            Self::PoolCreate {
                source, ..
            } => source.to_errno(),
            Self::Export {
                source, ..
            } => source.to_errno(),
            Self::Destroy {
                ..
            } => Errno::ENXIO,
            Self::Grow {
                ..
            } => Errno::ENXIO,
            Self::PoolNotFound {
                source, ..
            } => source.to_errno(),
            Self::InvalidBdev {
                ..
            } => Errno::ENXIO,
            Self::Invalid {
                source, ..
            } => source.to_errno(),
            Self::InvalidClusterSize {
                source, ..
            } => source.to_errno(),
            Self::InvalidMetadataParam {
                ..
            } => Errno::EINVAL,
            Self::RepExists {
                source, ..
            } => source.to_errno(),
            Self::RepCreate {
                source, ..
            } => source.to_errno(),
            Self::RepDestroy {
                source, ..
            } => source.to_errno(),
            Self::RepResize {
                source, ..
            } => source.to_errno(),
            Self::NotALvol {
                source, ..
            } => source.to_errno(),
            Self::LvolShare {
                source, ..
            } => source.to_errno(),
            Self::UpdateShareProperties {
                source, ..
            } => source.to_errno(),
            Self::LvolUnShare {
                source, ..
            } => source.to_errno(),
            Self::GetProperty {
                source, ..
            } => source.to_errno(),
            Self::SetProperty {
                source, ..
            } => source.to_errno(),
            Self::SyncProperty {
                source, ..
            } => source.to_errno(),
            Self::SnapshotCreate {
                source, ..
            } => source.to_errno(),
            Self::FlushFailed {
                ..
            } => Errno::EIO,
            Self::Property {
                source, ..
            } => source.to_errno(),
            Self::SnapshotConfigFailed {
                ..
            }
            | Self::ReplicaShareProtocol {
                ..
            } => Errno::EINVAL,
            Self::SnapshotCloneCreate {
                source, ..
            } => source.to_errno(),
            Self::CloneConfigFailed {
                ..
            } => Errno::EINVAL,
            Self::WipeFailed {
                ..
            } => Errno::EINVAL,
            Self::ResourceLockFailed {
                ..
            } => Errno::EBUSY,
        }
    }
}

impl From<crate::core::wiper::Error> for LvsError {
    fn from(source: crate::core::wiper::Error) -> Self {
        Self::WipeFailed {
            source,
        }
    }
}
