use async_trait::async_trait;
use snafu::ResultExt;
use std::pin::Pin;

use super::{
    Error,
    NbdDisk,
    Nexus,
    NexusTarget,
    ShareNbdNexus,
    ShareNvmfNexus,
    UnshareNexus,
};

use crate::core::{Protocol, Share};

///
/// The sharing of the nexus is different compared to regular bdevs
/// the Impl of ['Share'] handles this accordingly
///
/// The nexus and replicas are typically shared over different
/// endpoints (not targets) however, we want to avoid too many
/// protocol specifics and for bdevs the need for different endpoints
/// is not implemented yet as the need for it has not arrived yet.
#[async_trait(? Send)]
impl<'n> Share for Nexus<'n> {
    type Error = Error;
    type Output = String;

    async fn share_nvmf(
        mut self: Pin<&mut Self>,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error> {
        let uri = match self.shared() {
            Some(Protocol::Off) | None => {
                info!("{:?}: sharing NVMF target...", self);

                let name = self.name.clone();
                self.as_mut()
                    .pin_bdev_mut()
                    .share_nvmf(cntlid_range)
                    .await
                    .context(ShareNvmfNexus {
                        name,
                    })?;

                let uri = self.share_uri().unwrap();
                info!("{:?}: shared NVMF target as '{}'", self, uri);
                uri
            }
            Some(Protocol::Nvmf) => {
                let uri = self.share_uri().unwrap();
                info!("{:?}: already shared as '{}'", self, uri);
                uri
            }
        };

        Ok(uri)
    }

    /// TODO
    async fn unshare(mut self: Pin<&mut Self>) -> Result<(), Self::Error> {
        info!("{:?}: unsharing nexus bdev...", self);

        let name = self.name.clone();
        self.as_mut()
            .pin_bdev_mut()
            .unshare()
            .await
            .context(UnshareNexus {
                name,
            })?;

        info!("{:?}: unshared nexus bdev", self);

        Ok(())
    }

    /// TODO
    fn shared(&self) -> Option<Protocol> {
        unsafe { self.bdev().shared() }
    }

    /// TODO
    fn share_uri(&self) -> Option<String> {
        unsafe { self.bdev().share_uri() }
    }

    /// TODO
    fn bdev_uri(&self) -> Option<String> {
        unsafe { self.bdev().bdev_uri() }
    }

    /// TODO
    fn bdev_uri_original(&self) -> Option<String> {
        unsafe { self.bdev().bdev_uri_original() }
    }
}

impl From<&NexusTarget> for Protocol {
    fn from(target: &NexusTarget) -> Protocol {
        match target {
            NexusTarget::NexusNvmfTarget => Protocol::Nvmf,
            _ => Protocol::Off,
        }
    }
}

impl<'n> Nexus<'n> {
    /// TODO
    pub async fn share(
        mut self: Pin<&mut Self>,
        protocol: Protocol,
        _key: Option<String>,
    ) -> Result<String, Error> {
        // This function should be idempotent as it's possible that
        // we get called more than once for some odd reason.
        if let Some(target) = &self.nexus_target {
            // We're already shared ...
            if Protocol::from(target) == protocol {
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
            // right now Off is mapped to Nbd, will clean up the Nbd related
            // code once we refactor the rust tests that use nbd.
            Protocol::Off => {
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
            Protocol::Nvmf => {
                let args = Some((
                    self.nvme_params.min_cntlid,
                    self.nvme_params.max_cntlid,
                ));
                let uri = self.as_mut().share_nvmf(args).await?;

                unsafe {
                    self.as_mut().get_unchecked_mut().nexus_target =
                        Some(NexusTarget::NexusNvmfTarget);
                }
                Ok(uri)
            }
        }
    }

    /// TODO
    pub async fn unshare_nexus(mut self: Pin<&mut Self>) -> Result<(), Error> {
        match unsafe { self.as_mut().get_unchecked_mut().nexus_target.take() } {
            Some(NexusTarget::NbdDisk(disk)) => {
                info!("{:?}: destroying NBD device target...", self);
                disk.destroy();
            }
            Some(NexusTarget::NexusNvmfTarget) => {
                info!("{:?}: unsharing NVMF target...", self);
            }
            None => {
                // Try unshare nexus bdev anyway, just in case it was shared
                // via bdev API. It is no-op if bdev was not shared.
            }
        }

        self.as_mut().unshare().await
    }

    /// TODO
    pub fn get_share_uri(&self) -> Option<String> {
        match self.nexus_target {
            Some(NexusTarget::NbdDisk(ref disk)) => Some(disk.as_uri()),
            Some(NexusTarget::NexusNvmfTarget) => self.share_uri(),
            None => None,
        }
    }
}
