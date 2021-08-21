use crate::lvs::Error;
use async_trait::async_trait;
use pin_utils::core_reexport::fmt::Formatter;
use std::{convert::TryFrom, fmt::Display};

#[derive(Debug, PartialOrd, PartialEq)]
/// Indicates what protocol the bdev is shared as
pub enum Protocol {
    /// not shared by any of the variants
    Off,
    /// shared as NVMe-oF TCP
    Nvmf,
    /// shared as iSCSI
    Iscsi,
}

impl TryFrom<i32> for Protocol {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Off),
            1 => Ok(Self::Nvmf),
            2 => Ok(Self::Iscsi),
            // the gRPC code does not validate enum's so we have
            // to do it here
            _ => Err(Error::ReplicaShareProtocol { value }),
        }
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let p = match self {
            Self::Off => "Not shared",
            Self::Iscsi => "iSCSI",
            Self::Nvmf => "NVMe-oF TCP",
        };
        write!(f, "{}", p)
    }
}

#[async_trait(? Send)]
pub trait Share: std::fmt::Debug {
    type Error;
    type Output: std::fmt::Display + std::fmt::Debug;
    async fn share_iscsi(&self) -> Result<Self::Output, Self::Error>;
    async fn share_nvmf(
        &self,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error>;
    async fn unshare(&self) -> Result<Self::Output, Self::Error>;
    fn shared(&self) -> Option<Protocol>;
    fn share_uri(&self) -> Option<String>;
    fn bdev_uri(&self) -> Option<String>;
}
