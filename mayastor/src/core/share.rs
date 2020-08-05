use async_trait::async_trait;

#[derive(Debug, PartialOrd, PartialEq)]
pub enum Protocol {
    Nvmf,
    Iscsi,
}

#[async_trait(? Send)]
pub trait Share: std::fmt::Debug {
    type Error;
    type Output: std::fmt::Display + std::fmt::Debug;
    async fn share_iscsi(&self) -> Result<Self::Output, Self::Error>;
    async fn share_nvmf(self) -> Result<Self::Output, Self::Error>;
    async fn unshare(&self) -> Result<Self::Output, Self::Error>;
    fn shared(&self) -> Option<Protocol>;
    fn share_uri(&self) -> Option<String>;
    fn bdev_uri(&self) -> Option<String>;
}
