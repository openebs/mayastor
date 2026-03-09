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
    #[snafu(display("{source}"))]
    Generic { source: Errno },
    #[snafu(display("{}", Errno::EINVAL))]
    InvalidArgument {},
    #[snafu(display(": volume not found"))]
    LvolNotFound {},
    #[snafu(display(": volume already exists"))]
    VolAlreadyExists {},
    #[snafu(display(": volume is busy"))]
    VolBusy {},
    #[snafu(display("{}: cannot import LVS", Errno::EILSEQ))]
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
    #[snafu(display("{source}: crypto vbdev error"))]
    LvsCryptoVbdev { source: Errno },
}

impl BsError {
    /// Creates a `BsError` from an `Errno` value.
    pub fn from_errno(value: Errno) -> Self {
        match value {
            Errno::UnknownErrno => {
                // Unknown errno may indicate that the source negative i32 value
                // was passed instead of taking the abs.
                warn!("Blob store: got unknown errno");
                BsError::Generic { source: value }
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
            _ => BsError::Generic { source: value },
        }
    }

    /// Creates a `BsError` from a raw i32 errno value.
    pub fn from_i32(value: i32) -> Self {
        let r = Errno::from_raw(value.abs());

        if value < 0 {
            warn!("Blob store: negative errno passed: {r}");
        }

        Self::from_errno(r)
    }
}

impl ToErrno for BsError {
    fn to_errno(&self) -> Errno {
        match self {
            Self::Generic { source } => *source,
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
            Self::LvsCryptoVbdev { source } => *source,
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
    PoolCreate { source: BsError, name: String },
    #[snafu(display("{source}, failed to export pool {name}"))]
    Export { source: BsError, name: String },
    #[snafu(display("{source}, failed to destroy pool {name}"))]
    Destroy { source: BdevError, name: String },
    #[snafu(display("{source}, failed to grow pool {name}"))]
    Grow { source: BsError, name: String },
    #[snafu(display("{source}: {name}"))]
    InvalidBdev { source: BdevError, name: String },
    #[snafu(display("{source}: {msg}"))]
    Invalid { source: BsError, msg: String },
    #[snafu(display("invalid cluster-size {msg}, for pool {name}"))]
    InvalidClusterSize { name: String, msg: String },
    #[snafu(display("pool {name}: invalid metadata parameter: {msg}"))]
    InvalidMetadataParam { name: String, msg: String },
    #[snafu(display("{source}, lvol exists {name}"))]
    RepExists { source: BsError, name: String },
    #[snafu(display("{source}, failed to create lvol {name}"))]
    RepCreate { source: BsError, name: String },
    #[snafu(display("{source}, failed to destroy lvol {name}: {msg}"))]
    RepDestroy {
        source: BsError,
        name: String,
        msg: String,
    },
    #[snafu(display("failed to resize lvol {name}"))]
    RepResize { source: BsError, name: String },
    #[snafu(display("bdev {name} is not a lvol"))]
    NotALvol { name: String },
    #[snafu(display("{source}, failed to share lvol {name}"))]
    LvolShare { source: CoreError, name: String },
    #[snafu(display("{source}, failed to update share properties lvol {name}"))]
    UpdateShareProperties { source: CoreError, name: String },
    #[snafu(display("{source}, failed to unshare lvol {name}"))]
    LvolUnShare { source: CoreError, name: String },
    #[snafu(display("{source}, failed to get property {prop} from {name}"))]
    GetProperty {
        source: BsError,
        prop: PropName,
        name: String,
    },
    #[snafu(display("{source}, failed to set property {prop} on {name}"))]
    SetProperty {
        source: BsError,
        prop: String,
        name: String,
    },
    #[snafu(display("{source}, failed to sync properties {name}"))]
    SyncProperty { source: BsError, name: String },
    #[snafu(display("invalid property value: {name}"))]
    Property { name: String },
    #[snafu(display("invalid replica share protocol value: {value}"))]
    ReplicaShareProtocol { value: i32 },
    #[snafu(display("{source}, snapshot {msg} creation failed"))]
    SnapshotCreate { source: BsError, msg: String },
    #[snafu(display("{source}, snapshotClone {msg} creation failed"))]
    SnapshotCloneCreate { source: BsError, msg: String },
    #[snafu(display("flush Failed for replica {name}"))]
    FlushFailed { name: String },
    #[snafu(display("snapshot parameters for replica {name} is not correct: {msg}"))]
    SnapshotConfigFailed { name: String, msg: String },
    #[snafu(display("clone parameters for replica {name} are not correct: {msg}"))]
    CloneConfigFailed { name: String, msg: String },
    #[snafu(display("{source}, failed to wipe the replica {name}"))]
    WipeFailed { source: CoreError, name: String },
    #[snafu(display("failed to acquire resource lock, {msg}"))]
    ResourceLockFailed { msg: String },
    #[snafu(display("{msg}"))]
    MaxExpansionParse { msg: String },
    #[snafu(display("{source}, failed to rescan bdev {name}"))]
    BdevRescanFailed { source: BsError, name: String },
    #[snafu(display("pool Bdev not extended: {name}"))]
    BdevNotExtended { name: String },
    #[snafu(display("failed to resize crypto bdev: {name}"))]
    CryptoBdevNotResized { name: String },
}

/// Map CoreError to errno code.
impl ToErrno for LvsError {
    fn to_errno(&self) -> Errno {
        match self {
            Self::Import {
                source: crate::lvs::BsError::InvalidArgument {},
                reason,
                ..
            } => match reason {
                crate::lvs::ImportErrorReason::None => Errno::EINVAL,
                crate::lvs::ImportErrorReason::NameMismatch { .. } => Errno::EMEDIUMTYPE,
                crate::lvs::ImportErrorReason::NameClash { .. } => Errno::ENOTUNIQ,
                crate::lvs::ImportErrorReason::UuidMismatch { .. } => Errno::EMEDIUMTYPE,
            },
            Self::Import { source, .. } => source.to_errno(),
            Self::PoolCreate { source, .. } => source.to_errno(),
            Self::Export { source, .. } => source.to_errno(),
            Self::Destroy { .. } => Errno::ENXIO,
            Self::Grow { source, .. } => source.to_errno(),
            Self::InvalidBdev { .. } => Errno::ENXIO,
            Self::Invalid { source, .. } => source.to_errno(),
            Self::InvalidClusterSize { .. } => Errno::EINVAL,
            Self::InvalidMetadataParam { .. } => Errno::EINVAL,
            Self::RepExists { source, .. } => source.to_errno(),
            Self::RepCreate { source, .. } => source.to_errno(),
            Self::RepDestroy { source, .. } => source.to_errno(),
            Self::RepResize { source, .. } => source.to_errno(),
            Self::NotALvol { .. } => Errno::EINVAL,
            Self::LvolShare { source, .. } => source.to_errno(),
            Self::UpdateShareProperties { source, .. } => source.to_errno(),
            Self::LvolUnShare { source, .. } => source.to_errno(),
            Self::GetProperty { source, .. } => source.to_errno(),
            Self::SetProperty { source, .. } => source.to_errno(),
            Self::SyncProperty { source, .. } => source.to_errno(),
            Self::SnapshotCreate { source, .. } => source.to_errno(),
            Self::FlushFailed { .. } => Errno::EIO,
            Self::Property { .. } => Errno::EINVAL,
            Self::SnapshotConfigFailed { .. } | Self::ReplicaShareProtocol { .. } => Errno::EINVAL,
            Self::SnapshotCloneCreate { source, .. } => source.to_errno(),
            Self::CloneConfigFailed { .. } => Errno::EINVAL,
            Self::WipeFailed { source, .. } => source.to_errno(),
            Self::ResourceLockFailed { .. } => Errno::EBUSY,
            Self::MaxExpansionParse { .. } => Errno::EINVAL,
            Self::BdevRescanFailed { source, .. } => source.to_errno(),
            Self::BdevNotExtended { .. } => Errno::EOPNOTSUPP,
            Self::CryptoBdevNotResized { .. } => Errno::EBUSY,
        }
    }
}
