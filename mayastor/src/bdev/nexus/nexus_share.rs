use std::ffi::CString;

use futures::channel::oneshot;

use spdk_sys::create_crypto_disk;

use crate::{
    bdev::{
        bdev_lookup_by_name,
        nexus::{nexus_bdev::Nexus, nexus_nbd::Disk, Error},
    },
    executor::{cb_arg, done_cb},
};

/// we are using the multi buffer encryption implementation using CBC as the
/// algorithm
const CRYPTO_FLAVOUR: &str = "crypto_aesni_mb";

impl Nexus {
    /// Publish the nexus to system using nbd device and return the path to
    /// nbd device.
    pub async fn share(
        &mut self,
        key: Option<String>,
    ) -> Result<String, Error> {
        if self.nbd_disk.is_some() {
            return Err(Error::Exists);
        }

        assert_eq!(self.share_handle, None);

        let name = if let Some(key) = key {
            let name = format!("crypto-{}", self.name);

            // constant
            let flavour = CString::new(CRYPTO_FLAVOUR)?;
            // name of the crypto device
            let cname = CString::new(name.clone())?;
            // the the nexus device itself
            let base = CString::new(self.name.clone())?;
            // the keys to the castle
            let key = CString::new(key)?;

            let rc = unsafe {
                create_crypto_disk(
                    base.as_ptr(),
                    cname.as_ptr(),
                    flavour.as_ptr(),
                    key.as_ptr(),
                )
            };

            if rc != 0 {
                return Err(Error::CreateFailed);
            }
            name
        } else {
            self.name.clone()
        };

        // The share handle is the actual bdev that is shared through the
        // various protocols.
        self.share_handle = Some(name);
        if let Some(share_handle) = self.share_handle.as_ref() {
            match Disk::create(share_handle).await {
                Ok(disk) => {
                    let device_path = disk.get_path();
                    self.nbd_disk = Some(disk);
                    Ok(device_path)
                }
                Err(err) => {
                    self.share_handle.take();
                    Err(err)
                }
            }
        } else {
            Err(Error::ShareError("Unable to share bdev".into()))
        }
    }

    /// Undo share operation on nexus. To the chain of bdevs are all claimed
    /// where the top-level dev is claimed by the subsystem that exports the
    /// bdev. As such, we must first destroy the share and move our way down
    /// from there.
    pub async fn unshare(&mut self) -> Result<(), Error> {
        match self.nbd_disk.take() {
            Some(share) => {
                share.destroy();
                if let Some(bdev) = self.share_handle.take() {
                    if let Some(bdev) = bdev_lookup_by_name(&bdev) {
                        // if there share handle is the same as bdev name it implies there
                        // is no top level bdev, and we are done
                        if self.name == bdev.name() {
                            return Ok(());
                        }

                        let (s, r) = oneshot::channel::<u32>();
                        // currently, we only have the crypto vbdev
                        unsafe {
                            spdk_sys::delete_crypto_disk(
                                bdev.inner,
                                Some(done_cb),
                                cb_arg(s),
                            );
                        }

                        let rc = r.await.expect("crypto delete sender is gone");
                        if rc != 0 {
                            return Err(Error::Internal(format!(
                                "Failed to destroy crypto device error: {}",
                                rc
                            )));
                        }
                    }
                    Ok(())
                } else {
                    Err(Error::ShareError(format!(
                        "{}: failed to fully unshare self",
                        self.name
                    )))
                }
            }
            None => Err(Error::NotFound),
        }
    }

    /// Return path /dev/... under which the nexus is shared or None if not
    /// shared.
    pub fn get_share_path(&self) -> Option<String> {
        match self.nbd_disk {
            Some(ref disk) => Some(disk.get_path()),
            None => None,
        }
    }
}
