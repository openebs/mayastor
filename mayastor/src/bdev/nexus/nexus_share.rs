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
            ShareNexus,
        },
        nexus_nbd::Disk,
    },
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
};

use rpc::mayastor::{
    ShareProtocol,
};

/// we are using the multi buffer encryption implementation using CBC as the
/// algorithm
const CRYPTO_FLAVOUR: &str = "crypto_aesni_mb";

fn validate_frontend_protocol(share_protocol : ShareProtocol) -> Result<ShareProtocol, Error>  {
    match share_protocol {
        ShareProtocol::Nvmf => Ok(ShareProtocol::Nvmf),
        ShareProtocol::Iscsi => Ok(ShareProtocol::Iscsi),
        ShareProtocol::Nbd => Ok(ShareProtocol::Nbd),
        _ => Err(Error::InvalidShareProtocol {sp_value: share_protocol as i32}),
    }
}

impl Nexus {
    /// Publish the nexus to system using nbd device and return the path to
    /// nbd device.
    pub async fn share(
        &mut self,
        share_proto: ShareProtocol,
        key: Option<String>,
    ) -> Result<String, Error> {
        if self.nbd_disk.is_some() {
            return Err(Error::AlreadyShared {
                name: self.name.clone(),
            });
        }

        assert_eq!(self.share_handle, None);
        validate_frontend_protocol(share_proto)?;

        self.share_protocol = share_proto;

        // TODO for now we discard and ignore share_proto

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
        let disk = Disk::create(&name).await.context(ShareNexus {
            name: self.name.clone(),
        })?;
        let device_path = disk.get_path();
        self.share_handle = Some(name);
        self.nbd_disk = Some(disk);
        Ok(device_path)
    }

    /// Undo share operation on nexus. To the chain of bdevs are all claimed
    /// where the top-level dev is claimed by the subsystem that exports the
    /// bdev. As such, we must first destroy the share and move our way down
    /// from there.
    pub async fn unshare(&mut self) -> Result<(), Error> {
        match self.nbd_disk.take() {
            Some(disk) => {
                if validate_frontend_protocol(self.share_protocol).is_err() {
                    return Err(Error::NotShared { name: self.name.clone(),});
                }

                disk.destroy();
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
                        r.await
                            .expect("crypto delete sender is gone")
                            .context(DestroyCryptoBdev {
                                name: self.name.clone(),
                            })?;
                    }
                } else {
                    warn!("Missing bdev for a shared device");
                }
                self.share_protocol = ShareProtocol::None;
                Ok(())
            }
            None => Err(Error::NotShared {
                name: self.name.clone(),
            }),
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
