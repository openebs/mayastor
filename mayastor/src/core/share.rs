use async_trait::async_trait;
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::Display;

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

impl From<i32> for Protocol {
    fn from(p: i32) -> Self {
        match p {
            0 => Self::Off,
            1 => Self::Nvmf,
            2 => Self::Iscsi,
            // we have to handle the whole range of u32  here
            // We panic here because the gRPC interface should
            // have never deserialized into something that is invalid. A
            // different approach is would be to set this to
            // something that is invalid but that would
            // open the flood gates, to much and leak into the code base.
            _ => panic!("invalid share value"),
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
    async fn share_nvmf(&self) -> Result<Self::Output, Self::Error>;
    async fn unshare(&self) -> Result<Self::Output, Self::Error>;
    fn shared(&self) -> Option<Protocol>;
    fn share_uri(&self) -> Option<String>;
    fn bdev_uri(&self) -> Option<String>;
}
