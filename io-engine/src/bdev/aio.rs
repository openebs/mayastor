use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::CString,
    fmt::{Debug, Formatter},
    os::unix::fs::FileTypeExt,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;
use spdk_rs::{
    ffihelper::IntoCString,
    libspdk::{bdev_aio_delete, bdev_aio_rescan, create_aio_bdev},
};
use url::Url;

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    bdev_api::{self, BdevError},
    core::{UntypedBdev, VerboseError},
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
};

pub(super) struct Aio {
    name: String,
    alias: String,
    blk_size: u32,
    uuid: Option<uuid::Uuid>,
    rescan: bool,
}

impl Debug for Aio {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aio '{}', 'blk_size: {}'", self.name, self.blk_size)
    }
}

/// Convert a URI to an Aio "object"
impl TryFrom<&Url> for Aio {
    type Error = BdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(BdevError::InvalidUri {
                uri: url.to_string(),
                message: String::from("no path segments"),
            });
        }

        let path_is_blockdev = std::fs::metadata(url.path())
            .ok()
            .map_or(false, |meta| meta.file_type().is_block_device());

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let blk_size: u32 = match parameters.remove("blk_size") {
            Some(value) => {
                value.parse().context(bdev_api::IntParamParseFailed {
                    uri: url.to_string(),
                    parameter: String::from("blk_size"),
                    value: value.clone(),
                })?
            }
            None => {
                if path_is_blockdev {
                    0
                } else {
                    512
                }
            }
        };
        let uuid = uri::uuid(parameters.remove("uuid")).context(
            bdev_api::UuidParamParseFailed {
                uri: url.to_string(),
            },
        )?;

        let rescan = parameters.remove("rescan").is_some();

        reject_unknown_parameters(url, parameters)?;

        Ok(Aio {
            name: url.path().into(),
            alias: url.to_string(),
            blk_size,
            uuid,
            rescan,
        })
    }
}

impl GetName for Aio {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Aio {
    type Error = BdevError;

    /// Create an AIO bdev
    async fn create(&self) -> Result<String, Self::Error> {
        if let Some(bdev) = UntypedBdev::lookup_by_name(&self.name) {
            return if self.rescan {
                self.try_rescan(bdev)
            } else {
                Err(BdevError::BdevExists {
                    name: self.name.clone(),
                })
            };
        }

        debug!("{:?}: creating bdev", self);

        let cname = CString::new(self.get_name()).unwrap();

        let errno = unsafe {
            create_aio_bdev(
                cname.as_ptr(),
                cname.as_ptr(),
                self.blk_size,
                false,
                false,
            )
        };

        if errno != 0 {
            let err = BdevError::CreateBdevFailed {
                source: Errno::from_i32(errno.abs()),
                name: self.get_name(),
            };

            error!("{:?} error: {}", self, err.verbose());

            return Err(err);
        }

        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.name) {
            if let Some(uuid) = self.uuid {
                unsafe { bdev.set_raw_uuid(uuid.into()) };
            }

            if !bdev.add_alias(&self.alias) {
                warn!("{:?}: failed to add alias '{}'", self, self.alias);
            }

            return Ok(self.get_name());
        }

        Err(BdevError::BdevNotFound {
            name: self.get_name(),
        })
    }

    /// Destroy the given AIO bdev
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        debug!("{:?}: deleting", self);

        match UntypedBdev::lookup_by_name(&self.name) {
            Some(mut bdev) => {
                bdev.remove_alias(&self.alias);
                let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
                unsafe {
                    bdev_aio_delete(
                        (*bdev.unsafe_inner_ptr()).name,
                        Some(done_errno_cb),
                        cb_arg(sender),
                    );
                }
                receiver
                    .await
                    .context(bdev_api::BdevCommandCanceled {
                        name: self.get_name(),
                    })?
                    .context(bdev_api::DestroyBdevFailed {
                        name: self.get_name(),
                    })
            }
            None => Err(BdevError::BdevNotFound {
                name: self.get_name(),
            }),
        }
    }
}

impl Aio {
    fn try_rescan(
        &self,
        bdev: UntypedBdev,
    ) -> Result<String, <Self as CreateDestroy>::Error> {
        let before = bdev.num_blocks();

        debug!("{self:?}: rescanning existing AIO bdev ({before} blocks) ...");

        let cname = self.name.clone().into_cstring();

        let errno = unsafe {
            bdev_aio_rescan(cname.as_ptr() as *mut std::os::raw::c_char)
        };

        if errno != 0 {
            let err = BdevError::ResizeBdevFailed {
                source: Errno::from_i32(errno.abs()),
                name: self.name.clone(),
            };

            error!("{:?} error: {}", self, err.verbose());

            return Err(err);
        }

        let after = bdev.num_blocks();

        debug!(
            "{self:?}: rescanning existing AIO bdev okay: {before} -> {after} blocks"
        );

        Ok(self.name.clone())
    }
}
