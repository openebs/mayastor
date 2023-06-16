use nix::errno::Errno;
use snafu::Snafu;
use tonic::{Code, Status};

use super::{
    nexus_injection::InjectionError,
    ChildError,
    NbdError,
    NexusPauseState,
};

use crate::{
    bdev_api::BdevError,
    core::{CoreError, VerboseError},
    rebuild::RebuildError,
    subsys::NvmfError,
};

/// Common errors for nexus basic operations and child operations
/// which are part of nexus object.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub), context(suffix(false)), module(nexus_err))]
pub enum Error {
    #[snafu(display("Nexus {} does not exist", name))]
    NexusNotFound { name: String },
    #[snafu(display("Nexus {} exists and is initialising", name))]
    NexusInitialising { name: String },
    #[snafu(display("Invalid nexus uuid \"{}\"", uuid))]
    InvalidUuid { uuid: String },
    #[snafu(display(
        "Nexus uuid \"{}\" already exists for nexus \"{}\"",
        uuid,
        nexus
    ))]
    UuidExists { uuid: String, nexus: String },
    #[snafu(display("Nexus with name \"{}\" already exists", name))]
    NameExists { name: String },
    #[snafu(display("Invalid encryption key"))]
    InvalidKey {},
    #[snafu(display("Failed to create crypto bdev for nexus {}", name))]
    CreateCryptoBdev { source: Errno, name: String },
    #[snafu(display("Failed to destroy crypto bdev for nexus {}", name))]
    DestroyCryptoBdev { source: Errno, name: String },
    #[snafu(display(
        "The nexus {} has been already shared with a different protocol",
        name
    ))]
    AlreadyShared { name: String },
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
    #[snafu(display(
        "Failed to register IO device nexus {}: {}",
        name,
        source
    ))]
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
        "Child {} of nexus {} is too small: size = {} x {}",
        child,
        name,
        num_blocks,
        block_size
    ))]
    ChildTooSmall {
        child: String,
        name: String,
        num_blocks: u64,
        block_size: u64,
    },
    #[snafu(display("Children of nexus {} have mixed block sizes", name))]
    MixedBlockSizes { name: String },
    #[snafu(display("Child {} is incompatible with its (zoned) siblings", child))]
    MixedZonedChild { child: String },
    #[snafu(display(
        "Child {} of nexus {} has incompatible size or block size",
        child,
        name
    ))]
    ChildGeometry { child: String, name: String },
    #[snafu(display("Child {} of nexus {} cannot be found", child, name))]
    ChildMissing { child: String, name: String },
    #[snafu(display("Child {} of nexus {} has no error store", child, name))]
    ChildMissingErrStore { child: String, name: String },
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
    #[snafu(display(
        "Cannot delete the last child {} of nexus {}",
        child,
        name
    ))]
    RemoveLastChild { child: String, name: String },
    #[snafu(display(
        "Cannot remove or offline the last child {} of nexus {}",
        child,
        name
    ))]
    RemoveLastHealthyChild { child: String, name: String },
    #[snafu(display(
        "Cannot remove or offline the last healthy child {} of nexus {}",
        child,
        name
    ))]
    ChildNotFound { child: String, name: String },
    #[snafu(display("Child {} of nexus {} is not open", child, name))]
    ChildDeviceNotOpen { child: String, name: String },
    #[snafu(display("Child {} of nexus {} already exists", child, name))]
    ChildAlreadyExists { child: String, name: String },
    #[snafu(display("Failed to pause child {} of nexus {}", child, name))]
    PauseChild { child: String, name: String },
    #[snafu(display("Suitable rebuild source for nexus {} not found", name))]
    NoRebuildSource { name: String },
    #[snafu(display(
        "Failed to create rebuild job for child {} of nexus {}",
        child,
        name,
    ))]
    CreateRebuild {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Rebuild job not found for child {} of nexus {}",
        child,
        name,
    ))]
    RebuildJobNotFound { child: String, name: String },
    #[snafu(display(
        "Rebuild job already exists for child {} of nexus {}",
        child,
        name,
    ))]
    RebuildJobAlreadyExists { child: String, name: String },
    #[snafu(display(
        "Failed to execute rebuild operation on job {} of nexus {}",
        job,
        name,
    ))]
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
    #[snafu(display(
        "Child {} of nexus {} is not degraded but {}",
        child,
        name,
        state
    ))]
    ChildNotDegraded {
        child: String,
        name: String,
        state: String,
    },
    #[snafu(display("Failed to get BdevHandle for snapshot operation"))]
    FailedGetHandle,
    #[snafu(display(
        "Failed to create snapshot on nexus {}: {}",
        name,
        reason
    ))]
    FailedCreateSnapshot { name: String, reason: String },
    #[snafu(display("NVMf subsystem error: {}", e))]
    SubsysNvmf { e: String },
    #[snafu(display("failed to pause {} current state {:?}", name, state))]
    Pause {
        state: NexusPauseState,
        name: String,
    },
    #[snafu(display("Nexus '{}': bad fault injection", name))]
    BadFaultInjection {
        source: InjectionError,
        name: String,
    },
    #[snafu(display("Operation not allowed: {}", reason))]
    OperationNotAllowed { reason: String },
    #[snafu(display("Invalid value for nvme reservation: {}", reservation))]
    InvalidReservation { reservation: u8 },
    #[snafu(display("failed to update share properties {}", name))]
    UpdateShareProperties { source: CoreError, name: String },
    #[snafu(display("Replication for zoned storage is not implemented. Consider adding a single zoned storage device to the nexus"))]
    ZonedReplicationNotImplemented,
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
        match e {
            Error::InvalidUuid {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::InvalidKey {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::InvalidShareProtocol {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::InvalidReservation {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::AlreadyShared {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::NotShared {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::NotSharedNvmf {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::CreateChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::MixedBlockSizes {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::ChildGeometry {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::ChildTooSmall {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::OpenChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::OperationNotAllowed {
                ..
            } => Status::failed_precondition(e.to_string()),
            Error::RemoveLastChild {
                ..
            } => Status::failed_precondition(e.to_string()),
            Error::RemoveLastHealthyChild {
                ..
            } => Status::failed_precondition(e.to_string()),
            Error::ChildNotFound {
                ..
            } => Status::not_found(e.to_string()),
            Error::RebuildJobNotFound {
                ..
            } => Status::not_found(e.to_string()),
            Error::NexusNotFound {
                ..
            } => Status::not_found(e.to_string()),
            Error::ChildAlreadyExists {
                ..
            } => Status::already_exists(e.to_string()),
            Error::NameExists {
                ..
            } => Status::already_exists(e.to_string()),
            e => Status::new(Code::Internal, e.verbose()),
        }
    }
}
