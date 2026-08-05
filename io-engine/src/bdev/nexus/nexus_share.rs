use crate::{bdev::PtplFileOps, core::UnshareProps};
use async_trait::async_trait;
use snafu::ResultExt;
use std::pin::Pin;

use super::{nexus_err, Error, NbdDisk, Nexus, NexusTarget, PersistOp};

use crate::{
    core::{NvmfShareProps, Protocol, PtplProps, Share, UpdateProps},
    subsys::NvmfSubsystem,
};

///
/// The sharing of the nexus is different compared to regular bdevs
/// the Impl of ['Share'] handles this accordingly
///
/// The nexus and replicas are typically shared over different
/// endpoints (not targets) however, we want to avoid too many
/// protocol specifics and for bdevs the need for different endpoints
/// is not implemented yet as the need for it has not arrived yet.
#[async_trait(? Send)]
impl Share for Nexus<'_> {
    type Error = Error;
    type Output = String;

    fn create_ptpl(&self) -> Result<Option<PtplProps>, Self::Error> {
        self.ptpl()
            .create()
            .map_err(|source| Error::ShareNvmfNexus {
                source: crate::core::CoreError::Ptpl {
                    reason: source.to_string(),
                },
                name: self.name.to_string(),
            })
    }

    async fn share_nvmf(
        mut self: Pin<&mut Self>,
        props: Option<NvmfShareProps>,
    ) -> Result<Self::Output, Self::Error> {
        let uri = match self.shared() {
            Some(Protocol::Off) | None => {
                info!("{:?}: sharing NVMF target...", self);

                // Persist `shared = true` before the share goes live, so a
                // crash from here cannot leave the nexus persisted as
                // unshared while initiators may already be writing data.
                self.as_mut()
                    .persist(PersistOp::SetShared { shared: true })
                    .await?;

                let name = self.name.clone();
                self.as_mut()
                    .pin_bdev_mut()
                    .share_nvmf(props)
                    .await
                    .context(nexus_err::ShareNvmfNexus { name })?;

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

    async fn update_properties<P: Into<Option<UpdateProps>>>(
        self: Pin<&mut Self>,
        props: P,
    ) -> Result<(), Self::Error> {
        let name = self.name.clone();
        self.pin_bdev_mut()
            .update_properties(props)
            .await
            .context(nexus_err::UpdateShareProperties { name })
    }

    /// TODO
    async fn unshare(
        mut self: Pin<&mut Self>,
        opts: Option<UnshareProps>,
    ) -> Result<(), Self::Error> {
        info!("{:?}: unsharing nexus bdev...", self);

        // Aborts frozen I/Os a priori
        // Note that this is not a foolproof solution, ie what if the nexus is still online
        // and then IOs become frozen after we start trying to unshare?
        // In practice this may not be the case since we only unshare when nothing else is
        // using the nexus, though what if there's a long IO getting stuck, and leading to
        // retire AND shutdown?
        self.abort_shutdown_frozen_ios().await;

        let name = self.name.clone();
        self.as_mut()
            .pin_bdev_mut()
            .unshare(opts)
            .await
            .context(nexus_err::UnshareNexus { name })?;

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

    fn allowed_hosts(&self) -> Vec<String> {
        unsafe { self.bdev().allowed_hosts() }
    }

    /// TODO
    fn bdev_uri(&self) -> Option<url::Url> {
        unsafe { self.bdev().bdev_uri() }
    }

    /// TODO
    fn bdev_uri_original(&self) -> Option<url::Url> {
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
        self: Pin<&mut Self>,
        protocol: Protocol,
        key: Option<String>,
    ) -> Result<String, Error> {
        self.share_ext(protocol, key, vec![], false).await
    }

    /// Share the nexus over the given protocol. When `read_only` is true the
    /// nexus is published in ROX mode: `nexus_io::submit_request` rejects
    /// write I/O at submit time so multiple initiators can safely share the
    /// target for read-only workloads. The flag is scoped to the current
    /// publish and is cleared on unshare, so re-sharing with a different
    /// value flips the mode.
    pub async fn share_ext(
        mut self: Pin<&mut Self>,
        protocol: Protocol,
        _key: Option<String>,
        allowed_hosts: Vec<String>,
        read_only: bool,
    ) -> Result<String, Error> {
        // This function should be idempotent as it's possible that
        // we get called more than once for some odd reason.
        if let Some(target) = &self.nexus_target {
            // We're already shared ...
            if Protocol::from(target) == protocol {
                // Same protocol as requested. `read_only` is negotiated with
                // the initiator at connect time (NVMe identify data is cached
                // per session), so a mid-life flip wouldn't propagate to
                // already-connected clients. Reject a mismatched value here
                // and require unshare + re-share to change the mode.
                if self.is_read_only() != read_only {
                    return Err(Error::ReadOnlyChangeNotAllowed {
                        name: self.name.clone(),
                        current: self.is_read_only(),
                    });
                }

                warn!("{} is already shared", self.name);

                self.as_mut()
                    .update_properties(UpdateProps::new().with_allowed_hosts(allowed_hosts))
                    .await?;

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
                // Persist `shared = true` before the NBD share goes live. The NVMe-oF
                // path persists the same transition inside `Share::share_nvmf`,
                // so each protocol owns its own transition guarantee.
                self.as_mut()
                    .persist(PersistOp::SetShared { shared: true })
                    .await?;

                let disk = NbdDisk::create(&self.name)
                    .await
                    .context(nexus_err::ShareNbdNexus {
                        name: self.name.clone(),
                    })?;
                let uri = disk.as_uri();
                unsafe {
                    self.as_mut().get_unchecked_mut().nexus_target =
                        Some(NexusTarget::NbdDisk(disk));
                }
                Ok(uri)
            }
            Protocol::Nvmf => {
                // Record the effective read-only state before the target goes
                // live, so any I/O that arrives from an initiator connecting
                // during the share window is subject to the ROX gate in
                // `nexus_io::submit_request`. `set_nexus_read_only` updates
                // the source-of-truth atomic and pushes the flag out to every
                // per-core channel so the submit path can read a plain bool.
                self.as_mut().set_nexus_read_only(read_only).await;

                let props = NvmfShareProps::new()
                    .with_range(Some((
                        self.nvme_params.min_cntlid,
                        self.nvme_params.max_cntlid,
                    )))
                    .with_ana(true)
                    .with_allowed_hosts(allowed_hosts)
                    .with_ptpl(self.create_ptpl()?);
                let uri = match self.as_mut().share_nvmf(Some(props)).await {
                    Ok(uri) => uri,
                    Err(e) => {
                        // Roll back the ROX flag if the share itself failed:
                        // the target never went live, so leaving `read_only`
                        // set would misreport state on a subsequent query.
                        self.as_mut().set_nexus_read_only(false).await;
                        return Err(e);
                    }
                };

                unsafe {
                    self.as_mut().get_unchecked_mut().nexus_target =
                        Some(NexusTarget::NexusNvmfTarget);
                }
                Ok(uri)
            }
        }
    }

    /// Unshare the nexus target and persist the now-unshared state.
    pub async fn unshare_nexus(mut self: Pin<&mut Self>) -> Result<(), Error> {
        self.as_mut().unshare_nexus_internal(true).await
    }

    /// Inner unshare path shared by the public `unshare_nexus` and the
    /// destroy path. `persist_clean` controls whether the now-unshared state
    /// is recorded; destroy passes `false` because `PersistOp::Shutdown`
    /// issued shortly afterwards persists shutdown markers directly.
    pub(super) async fn unshare_nexus_internal(
        mut self: Pin<&mut Self>,
        persist_clean: bool,
    ) -> Result<(), Error> {
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

        self.as_mut().unshare(None).await?;

        // Clear the ROX flag: without an active target no front-end I/O can
        // flow, so read-only doesn't apply here. Reset to `false` (the default
        // RWO shape) rather than remembering the last-published value. Uses
        // `set_nexus_read_only` so per-core channel snapshots come along.
        self.as_mut().set_nexus_read_only(false).await;

        if persist_clean {
            // Double-check the `NvmfSubsystem` is really gone before marking
            // the nexus as unshared. A racing resume can re-start a stopped
            // subsystem and leave the unshare looking successful on the way
            // out; if it is still around, front-end I/O can still be
            // possible, so leave the persisted shared marker as-is and let
            // the existing crash-recovery path handle it on next start.
            if NvmfSubsystem::nqn_lookup(&self.name).is_some() {
                warn!(
                    "{self:?}: NvmfSubsystem still present after unshare, \
                    skipping set-shared persist (subsystem resume race)"
                );
            } else if let Err(e) = self
                .as_mut()
                .persist(PersistOp::SetShared { shared: false })
                .await
            {
                warn!(
                    "{self:?}: failed to persist shared after unshare \
                    (best-effort, falling back to existing crash-recovery \
                    behaviour on next start): {e}"
                );
            }
        }

        Ok(())
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
        Self { uuid }
    }
    fn uuid(&self) -> &uuid::Uuid {
        &self.uuid
    }
}
impl<'n> From<&Nexus<'n>> for NexusPtpl {
    fn from(n: &Nexus<'n>) -> Self {
        NexusPtpl { uuid: n.uuid() }
    }
}
impl PtplFileOps for NexusPtpl {
    fn destroy(&self) -> Result<(), std::io::Error> {
        if let Some(path) = self.path() {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    fn subpath(&self) -> std::path::PathBuf {
        std::path::PathBuf::from("nexus/")
            .join(self.uuid().to_string())
            .with_extension("json")
    }
}
