use crate::lvm::Error as LvmError;
use tonic::Status;

impl From<LvmError> for Status {
    fn from(e: LvmError) -> Self {
        match e {
            LvmError::InvalidPoolType {
                ..
            }
            | LvmError::VgUuidSet {
                ..
            }
            | LvmError::DisksMismatch {
                ..
            } => Status::invalid_argument(e.to_string()),
            LvmError::NotFound {
                ..
            }
            | LvmError::LvNotFound {
                ..
            } => Status::not_found(e.to_string()),
            LvmError::NoSpace {
                ..
            } => Status::resource_exhausted(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}
