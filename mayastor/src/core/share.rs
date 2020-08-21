use async_trait::async_trait;
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::Display;

#[derive(Debug, PartialOrd, PartialEq)]
pub enum Protocol {
    None,
    Nvmf,
    Iscsi,
    Invalid,
}

impl From<i32> for Protocol {
    fn from(p: i32) -> Self {
        match p {
            0 => Self::None,
            1 => Self::Nvmf,
            2 => Self::Iscsi,
            _ => Self::Invalid,
        }
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let p = match self {
            Self::None => "Not shared",
            Self::Iscsi => "iSCSI",
            Self::Nvmf => "NVMe-oF TCP",
            Self::Invalid => "Invalid protocol",
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
