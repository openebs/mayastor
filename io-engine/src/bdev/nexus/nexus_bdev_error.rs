use nix::errno::Errno;
use snafu::Snafu;
use tonic::Status;

use super::{ChildError, NbdError};

use crate::{
    bdev_api::BdevError,
    core::{CoreError, ToErrno, VerboseError},
    rebuild::RebuildError,
    store::store_defs::StoreError,
    subsys::NvmfError,
};

/// Common errors for nexus basic operations and child operations
/// which are part of nexus object.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(nexus_err))]
pub enum Error {
    #[snafu(display("Nexus {} does not exist", name))]
    NexusNotFound { name: String },
    #[snafu(display("Nexus {} exists and is initialising", name))]
    NexusInitialising { name: String },
    #[snafu(display("Invalid nexus uuid \"{}\"", uuid))]
    InvalidUuid { uuid: String },
    #[snafu(display("Nexus uuid \"{}\" already exists for nexus \"{}\"", uuid, nexus))]
    UuidExists { uuid: String, nexus: String },
    #[snafu(display("Nexus with name \"{}\" already exists", name))]
    NameExists { name: String },
    #[snafu(display("Invalid encryption key"))]
    InvalidKey {},
    #[snafu(display("The nexus {} has been already shared with a different protocol", name))]
    AlreadyShared { name: String },
    #[snafu(display(
        "The nexus {} is already shared as {} (read_only={}); \
         unshare and re-share to change the read_only flag",
        name,
        "Nvmf",
        current
    ))]
    ReadOnlyChangeNotAllowed { name: String, current: bool },
    #[snafu(display("The nexus {} has not been shared", name))]
    NotShared { name: String },
    #[snafu(display("The nexus {} has not been shared over NVMf", name))]
    NotSharedNvmf { name: String },
    #[snafu(display("Failed to share nexus over NBD {}", name))]
    ShareNbdNexus { source: NbdError, name: String },
    #[snafu(display("Failed to share nvmf nexus {}", name))]
    ShareNvmfNexus { source: CoreError, name: String },
    #[snafu(display("Failed to unshare nexus {}", name))]
    UnshareNexus { source: CoreError, name: String },
    #[snafu(display("Failed to register IO device nexus {}: {}", name, source))]
    RegisterNexus { source: Errno, name: String },
    #[snafu(display("Failed to create child of nexus {}: {}", name, source))]
    CreateChild { source: BdevError, name: String },
    #[snafu(display(
        "Deferring open because nexus {} is incomplete because {}",
        name,
        reason
    ))]
    NexusIncomplete { name: String, reason: String },
    #[snafu(display(
        "Child {} of nexus {} is too small: size = {} x {}, required = {} x {}",
        child,
        name,
        num_blocks,
        block_size,
        req_blocks,
        block_size
    ))]
    ChildTooSmall {
        child: String,
        name: String,
        num_blocks: u64,
        block_size: u64,
        req_blocks: u64,
    },
    #[snafu(display("Children of nexus {} have mixed block sizes", name))]
    MixedBlockSizes { name: String },
    #[snafu(display(
        "Child {} of nexus {} has incompatible size or block size",
        child,
        name
    ))]
    ChildGeometry { child: String, name: String },
    #[snafu(display("Child {} of nexus {} cannot be found", child, name))]
    ChildMissing { child: String, name: String },
    #[snafu(display(
        "Failed to acquire write exclusive reservation on child {} of nexus {}",
        child,
        name
    ))]
    ChildWriteExclusiveResvFailed {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display("Failed to open child {} of nexus {}", child, name))]
    OpenChild {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display("Failed to online child {} of nexus {}", child, name))]
    OnlineChild {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display("Failed to close child {} of nexus {}", child, name))]
    CloseChild {
        source: BdevError,
        child: String,
        name: String,
    },
    #[snafu(display("Cannot delete the last child {} of nexus {}", child, name))]
    RemoveLastChild { child: String, name: String },
    #[snafu(display("Cannot remove or offline the last child {} of nexus {}", child, name))]
    RemoveLastHealthyChild { child: String, name: String },
    #[snafu(display("Child {} of nexus {} not found", child, name))]
    ChildNotFound { child: String, name: String },
    #[snafu(display("Child {} of nexus {} is not open", child, name))]
    ChildDeviceNotOpen { child: String, name: String },
    #[snafu(display("Child {} of nexus {} already exists", child, name))]
    ChildAlreadyExists { child: String, name: String },
    #[snafu(display("Suitable rebuild source for nexus {} not found", name))]
    NoRebuildSource { name: String },
    #[snafu(display("Failed to create rebuild job for child {} of nexus {}", child, name,))]
    CreateRebuild {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display("Rebuild job not found for child {} of nexus {}", child, name,))]
    RebuildJobNotFound { child: String, name: String },
    #[snafu(display("Rebuild job already exists for child {} of nexus {}", child, name,))]
    RebuildJobAlreadyExists { child: String, name: String },
    #[snafu(display("Failed to execute rebuild operation on job {} of nexus {}", job, name,))]
    RebuildOperation {
        job: String,
        name: String,
        source: RebuildError,
    },
    #[snafu(display("Invalid ShareProtocol value {}", sp_value))]
    InvalidShareProtocol { sp_value: i32 },
    #[snafu(display("Invalid NvmeAnaState value {}", ana_value))]
    InvalidNvmeAnaState { ana_value: i32 },
    #[snafu(display("Invalid arguments for nexus {}: {}", name, args))]
    InvalidArguments { name: String, args: String },
    #[snafu(display("Failed to create nexus {} because {}", name, reason))]
    NexusCreate { name: String, reason: String },
    #[snafu(display("Failed to destroy nexus {}", name))]
    NexusDestroy { name: String },
    #[snafu(display("Failed to resize nexus {}", name))]
    NexusResize { source: Errno, name: String },
    #[snafu(display("Child {} of nexus {} is not degraded but {}", child, name, state))]
    ChildNotDegraded {
        child: String,
        name: String,
        state: String,
    },
    #[snafu(display("Failed to create snapshot on nexus {}: {}", name, reason))]
    FailedCreateSnapshot { name: String, reason: String },
    #[snafu(display("NVMf subsystem error: {}", e))]
    SubsysNvmf { e: String },
    #[snafu(display("Operation not allowed: {}", reason))]
    OperationNotAllowed { reason: String },
    #[snafu(display("Invalid value for nvme reservation: {}", reservation))]
    InvalidReservation { reservation: u8 },
    #[snafu(display("failed to update share properties {}", name))]
    UpdateShareProperties { source: CoreError, name: String },
    #[snafu(display("failed to save nexus state {name}, {source}"))]
    SaveStateFailed { source: StoreError, name: String },
    #[snafu(display("Nexus {name} requested {requested}B but got {got}B"))]
    NexusSizeUnmatched {
        name: String,
        requested: u64,
        got: u64,
    },
}

impl From<NvmfError> for Error {
    fn from(error: NvmfError) -> Self {
        Error::SubsysNvmf {
            e: error.to_string(),
        }
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        let errno = e.to_errno();
        let mut status = match e {
            Error::InvalidUuid { .. } => Status::invalid_argument(e.to_string()),
            Error::InvalidKey { .. } => Status::invalid_argument(e.to_string()),
            Error::InvalidShareProtocol { .. } => Status::invalid_argument(e.to_string()),
            Error::InvalidReservation { .. } => Status::invalid_argument(e.to_string()),
            Error::AlreadyShared { .. } => Status::invalid_argument(e.to_string()),
            Error::ReadOnlyChangeNotAllowed { .. } => Status::failed_precondition(e.to_string()),
            Error::NotShared { .. } => Status::invalid_argument(e.to_string()),
            Error::NotSharedNvmf { .. } => Status::invalid_argument(e.to_string()),
            Error::CreateChild { .. } => Status::invalid_argument(e.to_string()),
            Error::MixedBlockSizes { .. } => Status::invalid_argument(e.to_string()),
            Error::ChildGeometry { .. } => Status::invalid_argument(e.to_string()),
            Error::ChildTooSmall { .. } => Status::invalid_argument(e.to_string()),
            Error::OpenChild { .. } => Status::invalid_argument(e.to_string()),
            Error::OperationNotAllowed { .. } => Status::failed_precondition(e.to_string()),
            Error::RemoveLastChild { .. } => Status::failed_precondition(e.to_string()),
            Error::RemoveLastHealthyChild { .. } => Status::failed_precondition(e.to_string()),
            Error::ChildNotFound { .. } => Status::not_found(e.to_string()),
            Error::RebuildJobNotFound { .. } => Status::not_found(e.to_string()),
            Error::NexusIncomplete { .. } => Status::failed_precondition(e.verbose()),
            Error::NexusResize { .. } => Status::failed_precondition(e.to_string()),
            Error::NexusNotFound { .. } => Status::not_found(e.to_string()),
            Error::ChildAlreadyExists { .. } => Status::already_exists(e.to_string()),
            Error::NameExists { .. } => Status::already_exists(e.to_string()),
            Error::InvalidArguments { .. } => Status::invalid_argument(e.to_string()),
            Error::ShareNvmfNexus { ref source, .. } if source.to_errno() == Errno::EMLINK => {
                Status::out_of_range(e.to_string())
            }
            Error::ShareNvmfNexus { .. } => Status::internal(e.to_string()),
            Error::NexusInitialising { .. } => Status::internal(e.to_string()),
            Error::UuidExists { .. } => Status::internal(e.to_string()),
            Error::ShareNbdNexus { .. } => Status::internal(e.to_string()),
            Error::UnshareNexus { .. } => Status::internal(e.to_string()),
            Error::RegisterNexus { .. } => Status::internal(e.to_string()),
            Error::ChildMissing { .. } => Status::internal(e.to_string()),
            Error::ChildWriteExclusiveResvFailed { .. } => Status::internal(e.to_string()),
            Error::OnlineChild { .. } => Status::internal(e.to_string()),
            Error::CloseChild { .. } => Status::internal(e.to_string()),
            Error::ChildDeviceNotOpen { .. } => Status::failed_precondition(e.to_string()),
            Error::NoRebuildSource { .. } => Status::failed_precondition(e.to_string()),
            Error::CreateRebuild { .. } => Status::already_exists(e.to_string()),
            Error::RebuildJobAlreadyExists { .. } => Status::already_exists(e.to_string()),
            Error::RebuildOperation { .. } => Status::internal(e.to_string()),
            Error::InvalidNvmeAnaState { .. } => Status::invalid_argument(e.to_string()),
            Error::NexusCreate { .. } => Status::internal(e.to_string()),
            Error::NexusDestroy { .. } => Status::internal(e.to_string()),
            Error::ChildNotDegraded { .. } => Status::failed_precondition(e.to_string()),
            Error::FailedCreateSnapshot { .. } => Status::internal(e.to_string()),
            Error::SubsysNvmf { .. } => Status::internal(e.to_string()),
            Error::UpdateShareProperties { .. } => Status::internal(e.to_string()),
            Error::SaveStateFailed { .. } => Status::data_loss(e.to_string()),
            Error::NexusSizeUnmatched { .. } => Status::invalid_argument(e.to_string()),
        };
        status
            .metadata_mut()
            .insert("errno", tonic::metadata::MetadataValue::from(errno as i32));
        status
    }
}

impl ToErrno for Error {
    fn to_errno(&self) -> nix::Error {
        match self {
            Error::ShareNvmfNexus { source, .. } | Error::UnshareNexus { source, .. } => {
                source.to_errno()
            }
            Error::RegisterNexus { source, .. } => *source,
            // todo: need to change spdk_nvme_connect_async to include the probe_error callback
            // otherwise the source errno here is always -ENXIO for nvmx failures.
            Error::CreateChild { source, .. } => source.to_errno(),
            Error::ChildWriteExclusiveResvFailed { .. } => nix::Error::EKEYREJECTED,
            Error::OpenChild { source, .. } => source.to_errno(),
            Error::OnlineChild { source, .. } => source.to_errno(),
            Error::CloseChild { source, .. } => source.to_errno(),
            Error::CreateRebuild { .. } => nix::Error::EPIPE,
            Error::RebuildOperation { .. } => nix::Error::EPIPE,
            Error::NexusResize { source, .. } => *source,
            Error::UpdateShareProperties { source, .. } => source.to_errno(),
            Error::SaveStateFailed { .. } => nix::Error::ENODATA,

            Error::ShareNbdNexus { .. } => Errno::ENOTSUP,

            Error::NexusNotFound { .. }
            | Error::ChildMissing { .. }
            | Error::ChildNotFound { .. }
            | Error::RebuildJobNotFound { .. } => Errno::ENOENT,

            Error::UuidExists { .. }
            | Error::NameExists { .. }
            | Error::ChildAlreadyExists { .. }
            | Error::RebuildJobAlreadyExists { .. } => Errno::EEXIST,

            Error::InvalidUuid { .. }
            | Error::InvalidKey { .. }
            | Error::InvalidShareProtocol { .. }
            | Error::InvalidNvmeAnaState { .. }
            | Error::InvalidArguments { .. }
            | Error::InvalidReservation { .. } => Errno::EINVAL,

            Error::NexusInitialising { .. } => Errno::EBUSY,

            Error::AlreadyShared { .. }
            | Error::ReadOnlyChangeNotAllowed { .. }
            | Error::NotShared { .. }
            | Error::NotSharedNvmf { .. }
            | Error::OperationNotAllowed { .. }
            | Error::RemoveLastChild { .. }
            | Error::RemoveLastHealthyChild { .. }
            | Error::ChildNotDegraded { .. } => Errno::EPERM,

            Error::ChildDeviceNotOpen { .. }
            | Error::ChildGeometry { .. }
            | Error::ChildTooSmall { .. }
            | Error::SubsysNvmf { .. } => Errno::EIO,

            Error::NexusIncomplete { .. }
            | Error::NexusCreate { .. }
            | Error::NexusDestroy { .. }
            | Error::FailedCreateSnapshot { .. }
            | Error::NoRebuildSource { .. } => Errno::EFAULT,

            // This is something we'll have to handle better
            Error::MixedBlockSizes { .. } | Error::NexusSizeUnmatched { .. } => Errno::EINVAL,
        }
    }
}
