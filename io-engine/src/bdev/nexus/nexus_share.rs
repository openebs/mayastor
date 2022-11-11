use crate::bdev::PtplFileOps;
use async_trait::async_trait;
use snafu::ResultExt;
use std::pin::Pin;

use super::{nexus_err, Error, NbdDisk, Nexus, NexusTarget};

use crate::core::{Protocol, Share, ShareProps};

#[async_trait(? Send)]
///
/// The sharing of the nexus is different compared to regular bdevs
/// the Impl of ['Share'] handles this accordingly
///
/// The nexus and replicas are typically shared over different
/// endpoints (not targets) however, we want to avoid too many
/// protocol specifics and for bdevs the need for different endpoints
/// is not implemented yet as the need for it has not arrived yet.
impl<'n> Share for Nexus<'n> {
    type Error = Error;
    type Output = String;

    async fn share_nvmf(
        mut self: Pin<&mut Self>,
        props: Option<ShareProps>,
    ) -> Result<Self::Output, Self::Error> {
        match self.shared() {
            Some(Protocol::Off) | None => {
                let name = self.name.clone();
                self.as_mut()
                    .pin_bdev_mut()
                    .share_nvmf(props)
                    .await
                    .context(nexus_err::ShareNvmfNexus {
                        name,
                    })?;
            }
            Some(Protocol::Nvmf) => {}
        }
        Ok(self.share_uri().unwrap())
    }

    /// TODO
    async fn unshare(
        self: Pin<&mut Self>,
    ) -> Result<Self::Output, Self::Error> {
        let name = self.name.clone();
        self.pin_bdev_mut()
            .unshare()
            .await
            .context(nexus_err::UnshareNexus {
                name,
            })
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
                    nexus_err::ShareNbdNexus {
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
                let props = ShareProps::new()
                    .with_range(Some((
                        self.nvme_params.min_cntlid,
                        self.nvme_params.max_cntlid,
                    )))
                    .with_ana(true)
                    .with_ptpl(self.ptpl().create().map_err(|source| {
                        Error::ShareNvmfNexus {
                            source: crate::core::CoreError::Ptpl {
                                reason: source.to_string(),
                            },
                            name: self.name.to_string(),
                        }
                    })?);
                let uri = self.as_mut().share_nvmf(Some(props)).await?;

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
        info!("{:?}: unsharing nexus...", self);
        unsafe {
            match self.as_mut().get_unchecked_mut().nexus_target.take() {
                Some(NexusTarget::NbdDisk(disk)) => {
                    disk.destroy();
                }
                Some(NexusTarget::NexusNvmfTarget) => {
                    self.as_mut().unshare().await?;
                }
                None => {
                    info!("{:?}: not shared", self);
                }
            }
        }

        Ok(())
    }

    /// Shutdowns all shares.
    pub(crate) async fn destroy_shares(
        mut self: Pin<&mut Self>,
    ) -> Result<(), Error> {
        let _ = self.as_mut().unshare_nexus().await;
        assert_eq!(self.share_handle, None);

        // no-op when not shared and will be removed once the old share bits are
        // gone. Ignore device name provided in case of successful unsharing.
        self.as_mut().unshare().await.map(|_| ())
    }

    /// TODO
    pub fn get_share_uri(&self) -> Option<String> {
        match self.nexus_target {
            Some(NexusTarget::NbdDisk(ref disk)) => Some(disk.as_uri()),
            Some(NexusTarget::NexusNvmfTarget) => self.share_uri(),
            None => None,
        }
    }

    /// Get a `PtplFileOps` from `&self`.
    pub(crate) fn ptpl(&self) -> impl PtplFileOps {
        NexusPtpl::from(self)
    }
}

/// Nexus reservation persistence through power loss implementation.
pub(crate) struct NexusPtpl {
    uuid: uuid::Uuid,
}
impl NexusPtpl {
    /// Get a `Self` with the given uuid.
    pub(crate) fn new(uuid: uuid::Uuid) -> Self {
        Self {
            uuid,
        }
    }
    fn uuid(&self) -> &uuid::Uuid {
        &self.uuid
    }
}
impl<'n> From<&Nexus<'n>> for NexusPtpl {
    fn from(n: &Nexus<'n>) -> Self {
        NexusPtpl {
            uuid: n.uuid(),
        }
    }
}
impl PtplFileOps for NexusPtpl {
    fn destroy(&self) -> Result<(), std::io::Error> {
        if let Some(path) = self.path() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn subpath(&self) -> std::path::PathBuf {
        std::path::PathBuf::from("nexus/")
            .join(self.uuid().to_string())
            .with_extension("json")
    }
}
