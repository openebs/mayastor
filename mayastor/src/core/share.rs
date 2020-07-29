use crate::core::CoreError;

pub trait Share {
    fn share_iscsi(&self, port: u32) -> Result<(), CoreError>;
    fn share_nvmf(&self, port: u32) -> Result<(), CoreError>;
    fn unshare(&self) -> Result<(), CoreError>;
}
