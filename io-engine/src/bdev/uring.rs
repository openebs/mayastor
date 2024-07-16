use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::CString,
    os::unix::fs::FileTypeExt,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use snafu::ResultExt;
use url::Url;

use spdk_rs::libspdk::{create_uring_bdev, delete_uring_bdev};

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    bdev_api::{self, BdevError},
    core::UntypedBdev,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
};

#[derive(Debug)]
pub(super) struct Uring {
    name: String,
    alias: String,
    blk_size: u32,
    uuid: Option<uuid::Uuid>,
}

/// Convert a URI to an Uring "object"
impl TryFrom<&Url> for Uring {
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

        reject_unknown_parameters(url, parameters)?;

        Ok(Uring {
            name: url.path().into(),
            alias: url.to_string(),
            blk_size,
            uuid,
        })
    }
}

impl GetName for Uring {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Uring {
    type Error = BdevError;

    /// Create a uring bdev
    async fn create(&self) -> Result<String, Self::Error> {
        if UntypedBdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevError::BdevExists {
                name: self.get_name(),
            });
        }

        let cname = CString::new(self.get_name()).unwrap();

        if let Some(mut bdev) = UntypedBdev::checked_from_ptr(unsafe {
            create_uring_bdev(cname.as_ptr(), cname.as_ptr(), self.blk_size)
        }) {
            if let Some(uuid) = self.uuid {
                unsafe { bdev.set_raw_uuid(uuid.into()) };
            }

            if !bdev.add_alias(&self.alias) {
                error!(
                    "failed to add alias {} to device {}",
                    self.alias,
                    self.get_name()
                );
            }

            return Ok(bdev.name().to_string());
        }

        Err(BdevError::BdevNotFound {
            name: self.get_name(),
        })
    }

    /// Destroy the given uring bdev
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        match UntypedBdev::lookup_by_name(&self.name) {
            Some(mut bdev) => {
                bdev.remove_alias(&self.alias);
                let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
                unsafe {
                    delete_uring_bdev(
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
