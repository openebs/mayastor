use nix::errno::Errno;
use snafu::Snafu;

use super::PropName;

use crate::{
    bdev_api::BdevError,
    core::{CoreError, ToErrno},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum Error {
    #[snafu(display("failed to import pool {}", name))]
    Import {
        source: Errno,
        name: String,
    },
    #[snafu(display("errno: {} failed to create pool {}", source, name))]
    PoolCreate {
        source: Errno,
        name: String,
    },
    #[snafu(display("failed to export pool {}", name))]
    Export {
        source: Errno,
        name: String,
    },
    #[snafu(display("failed to destroy pool {}", name))]
    Destroy {
        source: BdevError,
        name: String,
    },
    #[snafu(display("{}", msg))]
    PoolNotFound {
        source: Errno,
        msg: String,
    },
    InvalidBdev {
        source: BdevError,
        name: String,
    },
    #[snafu(display("errno {}: {}", source, msg))]
    Invalid {
        source: Errno,
        msg: String,
    },
    #[snafu(display("lvol exists {}", name))]
    RepExists {
        source: Errno,
        name: String,
    },
    #[snafu(display("errno: {} failed to create lvol {}", source, name))]
    RepCreate {
        source: Errno,
        name: String,
    },
    #[snafu(display("failed to destroy lvol {}{}", name, if msg.is_empty() { "" } else { msg.as_str() }))]
    RepDestroy {
        source: Errno,
        name: String,
        msg: String,
    },
    #[snafu(display("bdev {} is not a lvol", name))]
    NotALvol {
        source: Errno,
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
        source: Errno,
        prop: PropName,
        name: String,
    },
    #[snafu(display("failed to set property {} on {}", prop, name))]
    SetProperty {
        source: Errno,
        prop: PropName,
        name: String,
    },
    #[snafu(display("failed to sync properties {}", name))]
    SyncProperty {
        source: Errno,
        name: String,
    },
    #[snafu(display("invalid property value: {}", name))]
    Property {
        source: Errno,
        name: String,
    },
    #[snafu(display("invalid replica share protocol value: {}", value))]
    ReplicaShareProtocol {
        value: i32,
    },
    #[snafu(display("Snapshot {} creation failed", msg))]
    SnapshotCreate {
        source: Errno,
        msg: String,
    },
    #[snafu(display("SnapshotClone {} creation failed", msg))]
    SnapshotCloneCreate {
        source: Errno,
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
}

/// Map CoreError to errno code.
impl ToErrno for Error {
    fn to_errno(self) -> Errno {
        match self {
            Self::Import {
                source, ..
            } => source,
            Self::PoolCreate {
                source, ..
            } => source,
            Self::Export {
                source, ..
            } => source,
            Self::Destroy {
                ..
            } => Errno::ENXIO,
            Self::PoolNotFound {
                source, ..
            } => source,
            Self::InvalidBdev {
                ..
            } => Errno::ENXIO,
            Self::Invalid {
                source, ..
            } => source,
            Self::RepExists {
                source, ..
            } => source,
            Self::RepCreate {
                source, ..
            } => source,
            Self::RepDestroy {
                source, ..
            } => source,
            Self::NotALvol {
                source, ..
            } => source,
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
            } => source,
            Self::SetProperty {
                source, ..
            } => source,
            Self::SyncProperty {
                source, ..
            } => source,
            Self::SnapshotCreate {
                source, ..
            } => source,
            Self::FlushFailed {
                ..
            } => Errno::EIO,
            Self::Property {
                source, ..
            } => source,
            Self::SnapshotConfigFailed {
                ..
            }
            | Self::ReplicaShareProtocol {
                ..
            } => Errno::EINVAL,
            Self::SnapshotCloneCreate {
                source, ..
            } => source,
            Self::CloneConfigFailed {
                ..
            } => Errno::EINVAL,
        }
    }
}
