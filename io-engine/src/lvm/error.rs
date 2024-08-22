use crate::core::ToErrno;
use nix::errno::Errno;
use snafu::Snafu;

/// Errors which can be encountered whilst using the LVM backend module.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Report missing from {command} output"))]
    ReportMissing { command: String },
    #[snafu(display("Failed to json parse {command} output: {error}"))]
    JsonParsing { command: String, error: String },
    #[snafu(display("{command} command failed: {error}"))]
    LvmBinErr { command: String, error: String },
    #[snafu(display("Failed to spawn/wait for {command} command: {source}"))]
    LvmBinSpawnErr {
        command: String,
        source: std::io::Error,
    },
    #[snafu(display(
        "LVM VolumeGroup disk mismatch, args:{args:?}, vg:{vg:?}"
    ))]
    DisksMismatch { args: Vec<String>, vg: Vec<String> },
    #[snafu(display("Invalid PoolType: {value}"))]
    InvalidPoolType { value: i32 },
    #[snafu(display("LVM VolumeGroup {query} not found"))]
    NotFound { query: String },
    #[snafu(display("Cannot set pool uuid to a user defined value"))]
    VgUuidSet {},
    #[snafu(display("Logical Volume with {query} not found"))]
    LvNotFound { query: String },
    #[snafu(display("Thin provisioned logical volumes not supported"))]
    ThinProv {},
    #[snafu(display("Failed to spawn reactor task"))]
    ReactorSpawn {},
    #[snafu(display("Failed to collect result of reactor spawn"))]
    ReactorSpawnChannel {},
    #[snafu(display("Failed to import the lvol as an spdk bdev: {source}"))]
    BdevImport { source: crate::bdev_api::BdevError },
    #[snafu(display("Failed to export the lvol's spdk bdev: {source}"))]
    BdevExport { source: crate::bdev_api::BdevError },
    #[snafu(display("Failed to open the lvol's spdk bdev: {source}"))]
    BdevOpen { source: crate::core::CoreError },
    #[snafu(display("{source}"))]
    BdevShare { source: crate::core::CoreError },
    #[snafu(display("Bdev is shared but no uri is found"))]
    BdevShareUri {},
    #[snafu(display("{source}"))]
    BdevUnshare { source: crate::core::CoreError },
    #[snafu(display("Bdev cannot be found after successful creation"))]
    BdevMissing {},
    #[snafu(display("Failed to update bdev's {name} properties: {source}"))]
    UpdateProps {
        source: crate::core::CoreError,
        name: String,
    },
    #[snafu(display("{error}"))]
    NoSpace { error: String },
    #[snafu(display("Snapshots are not currently supported for LVM volumes"))]
    SnapshotNotSup {},
    #[snafu(display(
        "Pool expansion is not currently supported for LVM volumes"
    ))]
    GrowNotSup {},
}

impl ToErrno for Error {
    fn to_errno(self) -> Errno {
        match self {
            Error::ReportMissing {
                ..
            } => Errno::EIO,
            Error::JsonParsing {
                ..
            } => Errno::EIO,
            Error::LvmBinErr {
                ..
            } => Errno::EIO,
            Error::LvmBinSpawnErr {
                ..
            } => Errno::EIO,
            Error::DisksMismatch {
                ..
            } => Errno::EINVAL,
            Error::InvalidPoolType {
                ..
            } => Errno::EINVAL,
            Error::NotFound {
                ..
            } => Errno::ENOENT,
            Error::VgUuidSet {
                ..
            } => Errno::EINVAL,
            Error::LvNotFound {
                ..
            } => Errno::ENOENT,
            Error::ThinProv {
                ..
            } => Errno::ENOTSUP,
            Error::ReactorSpawn {
                ..
            } => Errno::EXFULL,
            Error::ReactorSpawnChannel {
                ..
            } => Errno::EPIPE,
            Error::BdevImport {
                ..
            } => Errno::EIO,
            Error::BdevExport {
                ..
            } => Errno::EIO,
            Error::BdevOpen {
                ..
            } => Errno::EIO,
            Error::BdevShare {
                ..
            } => Errno::EFAULT,
            Error::BdevShareUri {
                ..
            } => Errno::EFAULT,
            Error::BdevUnshare {
                ..
            } => Errno::EFAULT,
            Error::BdevMissing {
                ..
            } => Errno::ENODEV,
            Error::UpdateProps {
                ..
            } => Errno::EIO,
            Error::NoSpace {
                ..
            } => Errno::ENOSPC,
            Error::SnapshotNotSup {
                ..
            } => Errno::ENOTSUP,
            Error::GrowNotSup {
                ..
            } => Errno::ENOTSUP,
        }
    }
}
