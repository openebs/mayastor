//!
//! The malloc bdev as the name implies, creates an in memory disk. Note
//! that the backing memory is allocated from huge pages and not from the
//! heap. IOW, you must ensure you do not run out of huge pages while using
//! this.
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Formatter},
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;
use url::Url;

use spdk_rs::{
    libspdk::{
        create_malloc_disk,
        delete_malloc_disk,
        malloc_bdev_opts,
        resize_malloc_disk,
        spdk_bdev,
        SPDK_DIF_DISABLE,
    },
    UntypedBdev,
};

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    bdev_api::{self, BdevError},
    core::VerboseError,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult, IntoCString},
};

pub struct Malloc {
    /// the name of the bdev we created, this is equal to the URI path minus
    /// the leading '/'
    name: String,
    /// alias which can be used to open the bdev
    alias: String,
    /// the number of blocks the device should have
    num_blocks: u64,
    /// the size of a single block if no blk_size is given we default to 512
    blk_size: u32,
    /// uuid of the spdk bdev
    uuid: Option<uuid::Uuid>,
    /// Enable resizing if the bdev already exists
    resizing: bool,
}

impl Debug for Malloc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Malloc '{}' ({} blocks)", self.name, self.num_blocks)
    }
}

impl TryFrom<&Url> for Malloc {
    type Error = BdevError;

    fn try_from(uri: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(uri);
        if segments.is_empty() {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "empty path".to_string(),
            });
        }

        let mut parameters: HashMap<String, String> =
            uri.query_pairs().into_owned().collect();

        let blk_size: u32 = if let Some(value) = parameters.remove("blk_size") {
            value.parse().context(bdev_api::IntParamParseFailed {
                uri: uri.to_string(),
                parameter: String::from("blk_size"),
                value: value.clone(),
            })?
        } else {
            512
        };

        let size: u32 = if let Some(value) = parameters.remove("size_mb") {
            value.parse().context(bdev_api::IntParamParseFailed {
                uri: uri.to_string(),
                parameter: String::from("size_mb"),
                value: value.clone(),
            })?
        } else {
            0
        };

        let num_blocks: u32 =
            if let Some(value) = parameters.remove("num_blocks") {
                value.parse().context(bdev_api::IntParamParseFailed {
                    uri: uri.to_string(),
                    parameter: String::from("num_blocks"),
                    value: value.clone(),
                })?
            } else {
                0
            };

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            bdev_api::UuidParamParseFailed {
                uri: uri.to_string(),
            },
        )?;

        let resizing = parameters.remove("resize").is_some();

        reject_unknown_parameters(uri, parameters)?;

        // Validate parameters.
        if blk_size != 512 && blk_size != 4096 {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "'blk_size' must be one of: 512, 4096".to_string(),
            });
        }

        if size != 0 && num_blocks != 0 {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "'num_blocks' and 'size_mb' are mutually exclusive"
                    .to_string(),
            });
        }

        if size == 0 && num_blocks == 0 {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "either 'num_blocks' or 'size_mb' must be specified"
                    .to_string(),
            });
        }

        Ok(Self {
            name: uri.path()[1 ..].into(),
            alias: uri.to_string(),
            num_blocks: if num_blocks != 0 {
                num_blocks
            } else {
                (size << 20) / blk_size
            } as u64,
            blk_size,
            uuid,
            resizing,
        })
    }
}

impl GetName for Malloc {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Malloc {
    type Error = BdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        if UntypedBdev::lookup_by_name(&self.name).is_some() {
            return if self.resizing {
                self.try_resize()
            } else {
                Err(BdevError::BdevExists {
                    name: self.name.clone(),
                })
            };
        }

        debug!("{:?}: creating bdev", self);

        let cname = self.name.clone().into_cstring();

        let errno = unsafe {
            let mut bdev: *mut spdk_bdev = std::ptr::null_mut();
            let opts = malloc_bdev_opts {
                name: cname.as_ptr() as *mut std::os::raw::c_char,
                uuid: Default::default(),
                num_blocks: self.num_blocks,
                block_size: self.blk_size,
                physical_block_size: 0,
                optimal_io_boundary: 0,
                md_size: 0,
                md_interleave: false,
                dif_type: SPDK_DIF_DISABLE,
                dif_is_head_of_md: false,
            };

            create_malloc_disk(&mut bdev, &opts)
        };

        if errno != 0 {
            let err = BdevError::CreateBdevFailed {
                source: Errno::from_i32(errno.abs()),
                name: self.name.clone(),
            };

            error!("{:?} error: {}", self, err.verbose());

            return Err(err);
        }

        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.name) {
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

            return Ok(self.name.clone());
        }

        Err(BdevError::BdevNotFound {
            name: self.name.clone(),
        })
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        debug!("{:?}: deleting", self);

        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.name) {
            bdev.remove_alias(&self.alias);
            let (s, r) = oneshot::channel::<ErrnoResult<()>>();

            unsafe {
                delete_malloc_disk(
                    (*bdev.unsafe_inner_ptr()).name,
                    Some(done_errno_cb),
                    cb_arg(s),
                );
            }

            r.await
                .context(bdev_api::BdevCommandCanceled {
                    name: self.name.clone(),
                })?
                .context(bdev_api::DestroyBdevFailed {
                    name: self.name,
                })
        } else {
            Err(BdevError::BdevNotFound {
                name: self.name,
            })
        }
    }
}

impl Malloc {
    fn try_resize(&self) -> Result<String, <Self as CreateDestroy>::Error> {
        debug!("{:?}: resizing existing bdev", self);

        let cname = self.name.clone().into_cstring();
        let new_sz_mb = self.num_blocks * self.blk_size as u64 / (1024 * 1024);

        let errno = unsafe {
            resize_malloc_disk(
                cname.as_ptr() as *mut std::os::raw::c_char,
                new_sz_mb,
            )
        };

        if errno != 0 {
            let err = BdevError::ResizeBdevFailed {
                source: Errno::from_i32(errno.abs()),
                name: self.name.clone(),
            };

            error!("{:?} error: {}", self, err.verbose());

            return Err(err);
        }

        Ok(self.name.clone())
    }
}
