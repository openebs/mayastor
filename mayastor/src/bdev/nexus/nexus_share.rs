use std::ffi::CString;

use futures::channel::oneshot;
use snafu::ResultExt;

use spdk_sys::create_crypto_disk;

use crate::{
    bdev::nexus::{
        nexus_bdev::{
            CreateCryptoBdev,
            DestroyCryptoBdev,
            Error,
            Nexus,
            NexusTarget,
            ShareIscsiNexus,
            ShareNbdNexus,
        },
        nexus_iscsi::NexusIscsiTarget,
        nexus_nbd::NbdDisk,
    },
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
};

use rpc::mayastor::ShareProtocolNexus;

/// we are using the multi buffer encryption implementation using CBC as the
/// algorithm
const CRYPTO_FLAVOUR: &str = "crypto_aesni_mb";

impl Nexus {
    pub async fn share(
        &mut self,
        share_protocol: ShareProtocolNexus,
        key: Option<String>,
    ) -> Result<String, Error> {
        // We could already be shared -- as CSI is idempotent chances are we get
        // called for some odd reason. Validate indeed -- that we are
        // shared by walking the target. If so, and the protocol is
        // correct simply return Ok(). If so, and the protocol is
        // incorrect, return Error(). If we are not shared but the
        // variant says we should be, carry on to correct the state.
        match self.nexus_target {
            Some(NexusTarget::NbdDisk(ref nbd_disk)) => {
                if share_protocol != ShareProtocolNexus::NexusNbd {
                    return Err(Error::AlreadyShared {
                        name: self.name.clone(),
                    });
                } else {
                    warn!("{} is already shared", self.name);
                    return Ok(nbd_disk.get_path());
                }
            }
            Some(NexusTarget::NexusIscsiTarget(ref iscsi_target)) => {
                if share_protocol != ShareProtocolNexus::NexusIscsi {
                    return Err(Error::AlreadyShared {
                        name: self.name.clone(),
                    });
                } else {
                    warn!("{} is already shared", self.name);
                    return Ok(iscsi_target.as_uri());
                }
            }
            None => (),
        }

        assert_eq!(self.share_handle, None);

        let name = if let Some(key) = key {
            let name = format!("crypto-{}", self.name);

            // constant
            let flavour = CString::new(CRYPTO_FLAVOUR).unwrap();
            // name of the crypto device
            let cname = CString::new(name.clone()).unwrap();
            // the nexus device itself
            let base = CString::new(self.name.clone()).unwrap();
            // the keys to the castle
            let key = CString::new(key).unwrap();

            let errno = unsafe {
                create_crypto_disk(
                    base.as_ptr(),
                    cname.as_ptr(),
                    flavour.as_ptr(),
                    key.as_ptr(),
                )
            };
            errno_result_from_i32(name, errno).context(CreateCryptoBdev {
                name: self.name.clone(),
            })?
        } else {
            self.name.clone()
        };

        debug!("creating share handle for {}", name);
        // The share handle is the actual bdev that is shared through the
        // various protocols.

        let device_id = match share_protocol {
            ShareProtocolNexus::NexusNbd => {
                // Publish the nexus to system using nbd device and return the
                // path to nbd device.
                let nbd_disk =
                    NbdDisk::create(&name).await.context(ShareNbdNexus {
                        name: self.name.clone(),
                    })?;
                let device_path = nbd_disk.get_path();
                self.nexus_target = Some(NexusTarget::NbdDisk(nbd_disk));
                device_path
            }
            ShareProtocolNexus::NexusIscsi => {
                // Publish the nexus to system using an iscsi target and return
                // the IQN
                let iscsi_target = NexusIscsiTarget::create(&name).context(
                    ShareIscsiNexus {
                        name: self.name.clone(),
                    },
                )?;
                let uri = iscsi_target.as_uri();
                self.nexus_target =
                    Some(NexusTarget::NexusIscsiTarget(iscsi_target));
                uri
            }
            ShareProtocolNexus::NexusNvmf => {
                return Err(Error::InvalidShareProtocol {
                    sp_value: share_protocol as i32,
                })
            }
        };
        self.share_handle = Some(name);
        Ok(device_id)
    }

    /// Undo share operation on nexus. To the chain of bdevs are all claimed
    /// where the top-level dev is claimed by the subsystem that exports the
    /// bdev. As such, we must first destroy the share and move our way down
    /// from there.
    pub async fn unshare(&mut self) -> Result<(), Error> {
        match self.nexus_target.take() {
            Some(NexusTarget::NbdDisk(disk)) => {
                disk.destroy();
            }
            Some(NexusTarget::NexusIscsiTarget(iscsi_target)) => {
                iscsi_target.destroy().await;
            }
            None => {
                warn!("{} was not shared", self.name);
                return Ok(());
            }
        };

        let bdev_name = self.share_handle.take().unwrap();
        if let Some(bdev) = Bdev::lookup_by_name(&bdev_name) {
            // if the share handle is the same as bdev name it
            // implies there is no top level bdev, and we are done
            if self.name != bdev.name() {
                let (s, r) = oneshot::channel::<ErrnoResult<()>>();
                // currently, we only have the crypto vbdev
                unsafe {
                    spdk_sys::delete_crypto_disk(
                        bdev.as_ptr(),
                        Some(done_errno_cb),
                        cb_arg(s),
                    );
                }
                r.await.expect("crypto delete sender is gone").context(
                    DestroyCryptoBdev {
                        name: self.name.clone(),
                    },
                )?;
            }
        } else {
            warn!("Missing bdev for a shared device");
        }
        Ok(())
    }

    /// Return path /dev/... under which the nexus is shared or None if not
    /// shared as nbd.
    pub fn get_share_path(&self) -> Option<String> {
        match self.nexus_target {
            Some(NexusTarget::NbdDisk(ref disk)) => Some(disk.get_path()),
            _ => None,
        }
    }
}
