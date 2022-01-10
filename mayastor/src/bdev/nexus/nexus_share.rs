use async_trait::async_trait;
use snafu::ResultExt;
use std::pin::Pin;

use rpc::mayastor::ShareProtocolNexus;

use super::{
    Error,
    NbdDisk,
    Nexus,
    NexusTarget,
    ShareIscsiNexus,
    ShareNbdNexus,
    ShareNvmfNexus,
    UnshareNexus,
};

use crate::core::{Protocol, Share};

#[async_trait(? Send)]
///
/// The sharing of the nexus is different compared to regular bdevs
/// the Impl of ['Share'] handles this accordingly
///
/// The nexus and replicas are typically shared over different
/// endpoints (not targets) however, we want to avoid too much
/// iSCSI specifics and for bdevs the need for different endpoints
/// is not implemented yet as the need for it has not arrived yet.
impl<'n> Share for Nexus<'n> {
    type Error = Error;
    type Output = String;

    async fn share_iscsi(&self) -> Result<Self::Output, Self::Error> {
        match self.shared() {
            Some(Protocol::Off) | None => {
                self.bdev().share_iscsi().await.context(ShareIscsiNexus {
                    name: self.name.clone(),
                })?;
            }
            Some(Protocol::Iscsi) => {}
            Some(protocol) => {
                error!("nexus {} already shared as {:?}", self.name, protocol);
                return Err(Error::AlreadyShared {
                    name: self.name.clone(),
                });
            }
        }
        Ok(self.share_uri().unwrap())
    }

    async fn share_nvmf(
        &self,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error> {
        match self.shared() {
            Some(Protocol::Off) | None => {
                self.bdev().share_nvmf(cntlid_range).await.context(
                    ShareNvmfNexus {
                        name: self.name.clone(),
                    },
                )?;
            }
            Some(Protocol::Nvmf) => {}
            Some(protocol) => {
                warn!("nexus {} already shared as {}", self.name, protocol);
                return Err(Error::AlreadyShared {
                    name: self.name.clone(),
                });
            }
        }
        Ok(self.share_uri().unwrap())
    }

    async fn unshare(&self) -> Result<Self::Output, Self::Error> {
        self.bdev().unshare().await.context(UnshareNexus {
            name: self.name.clone(),
        })
    }

    fn shared(&self) -> Option<Protocol> {
        self.bdev().shared()
    }

    fn share_uri(&self) -> Option<String> {
        self.bdev().share_uri()
    }

    fn bdev_uri(&self) -> Option<String> {
        self.bdev().bdev_uri()
    }

    fn bdev_uri_original(&self) -> Option<String> {
        self.bdev().bdev_uri_original()
    }
}

impl From<&NexusTarget> for ShareProtocolNexus {
    fn from(target: &NexusTarget) -> ShareProtocolNexus {
        match target {
            NexusTarget::NbdDisk(_) => ShareProtocolNexus::NexusNbd,
            NexusTarget::NexusIscsiTarget => ShareProtocolNexus::NexusIscsi,
            NexusTarget::NexusNvmfTarget => ShareProtocolNexus::NexusNvmf,
        }
    }
}

impl<'n> Nexus<'n> {
    pub async fn share(
        mut self: Pin<&mut Self>,
        protocol: ShareProtocolNexus,
        _key: Option<String>,
    ) -> Result<String, Error> {
        // This function should be idempotent as it's possible that
        // we get called more than once for some odd reason.
        if let Some(target) = &self.nexus_target {
            // We're already shared ...
            if ShareProtocolNexus::from(target) == protocol {
                // Same protocol as that requested, simply return Ok()
                warn!("{} is already shared", self.name);
                return Ok(self.get_share_uri().unwrap());
            }

            // Error as protocol differs from that requested.
            return Err(Error::AlreadyShared {
                name: self.name.clone(),
            });
        }

        match protocol {
            ShareProtocolNexus::NexusNbd => {
                let disk = NbdDisk::create(&self.name).await.context(
                    ShareNbdNexus {
                        name: self.name.clone(),
                    },
                )?;
                let uri = disk.as_uri();
                unsafe {
                    self.as_mut().get_unchecked_mut().nexus_target =
                        Some(NexusTarget::NbdDisk(disk));
                }
                Ok(uri)
            }
            ShareProtocolNexus::NexusIscsi => {
                let uri = self.share_iscsi().await?;
                unsafe {
                    self.as_mut().get_unchecked_mut().nexus_target =
                        Some(NexusTarget::NexusIscsiTarget);
                }
                Ok(uri)
            }
            ShareProtocolNexus::NexusNvmf => {
                let uri = self
                    .share_nvmf(Some((
                        self.nvme_params.min_cntlid,
                        self.nvme_params.max_cntlid,
                    )))
                    .await?;

                unsafe {
                    self.as_mut().get_unchecked_mut().nexus_target =
                        Some(NexusTarget::NexusNvmfTarget);
                }
                Ok(uri)
            }
        }
    }

    pub async fn unshare_nexus(mut self: Pin<&mut Self>) -> Result<(), Error> {
        unsafe {
            match self.as_mut().get_unchecked_mut().nexus_target.take() {
                Some(NexusTarget::NbdDisk(disk)) => {
                    disk.destroy();
                }
                Some(NexusTarget::NexusIscsiTarget) => {
                    self.unshare().await?;
                }
                Some(NexusTarget::NexusNvmfTarget) => {
                    self.unshare().await?;
                }
                None => {
                    warn!("{} was not shared", self.name);
                }
            }
        }

        Ok(())
    }

    pub fn get_share_uri(&self) -> Option<String> {
        match self.nexus_target {
            Some(NexusTarget::NbdDisk(ref disk)) => Some(disk.as_uri()),
            Some(NexusTarget::NexusIscsiTarget) => self.share_uri(),
            Some(NexusTarget::NexusNvmfTarget) => self.share_uri(),
            None => None,
        }
    }
}
