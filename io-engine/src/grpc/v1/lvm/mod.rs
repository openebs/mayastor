use crate::lvm::Error as LvmError;
use tonic::Status;

impl From<LvmError> for tonic::Status {
    fn from(e: LvmError) -> Self {
        match e {
            LvmError::InvalidPoolType { .. }
            | LvmError::VgUuidSet { .. }
            | LvmError::InvalidOption { .. }
            | LvmError::InvalidTagValue { .. }
            | LvmError::DisksMismatch { .. } => Status::invalid_argument(e.to_string()),
            LvmError::NotFound { .. }
            | LvmError::LvNotFound { .. }
            | LvmError::SnapNotFound { .. } => Status::not_found(e.to_string()),
            LvmError::NoSpace { .. } => Status::resource_exhausted(e.to_string()),
            LvmError::Exists { .. } => Status::already_exists(e.to_string()),
            LvmError::NoThinPool { .. }
            | LvmError::SnapshotThick { .. }
            | LvmError::NoThinPoolTarget { .. }
            | LvmError::HasLiveSnapshots { .. }
            | LvmError::SnapshotHasClones { .. } => Status::failed_precondition(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}
