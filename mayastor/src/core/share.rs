use crate::lvs::Error;
use async_trait::async_trait;
use pin_utils::core_reexport::fmt::Formatter;
use std::{convert::TryFrom, fmt::Display, pin::Pin};

#[derive(Debug, PartialOrd, PartialEq)]
/// Indicates what protocol the bdev is shared as
pub enum Protocol {
    /// not shared by any of the variants
    Off,
    /// shared as NVMe-oF TCP
    Nvmf,
}

impl TryFrom<i32> for Protocol {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Off),
            1 => Ok(Self::Nvmf),
            // 2 was for iSCSI
            // the gRPC code does not validate enums so we have
            // to do it here
            _ => Err(Error::ReplicaShareProtocol {
                value,
            }),
        }
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let p = match self {
            Self::Off => "Not shared",
            Self::Nvmf => "NVMe-oF TCP",
        };
        write!(f, "{}", p)
    }
}

#[async_trait(? Send)]
pub trait Share: std::fmt::Debug {
    type Error;
    type Output: std::fmt::Display + std::fmt::Debug;
    async fn share_nvmf(
        self: Pin<&mut Self>,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error>;

    /// TODO
    async fn unshare(self: Pin<&mut Self>)
        -> Result<Self::Output, Self::Error>;

    /// TODO
    fn shared(&self) -> Option<Protocol>;

    /// TODO
    fn share_uri(&self) -> Option<String>;

    /// TODO
    fn bdev_uri(&self) -> Option<String>;

    /// TODO
    fn bdev_uri_original(&self) -> Option<String>;
}
