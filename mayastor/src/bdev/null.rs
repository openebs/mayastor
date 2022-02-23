//! As the name implies, this is a dummy driver that discards all writes and
//! returns undefined data for reads. It's useful for benchmarking the I/O stack
//! with minimal overhead and should *NEVER* be used with *real* data.
use std::{collections::HashMap, convert::TryFrom};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;
use url::Url;
use uuid::Uuid;

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    core::UntypedBdev,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult, IntoCString},
    nexus_uri::{
        NexusBdevError,
        {self},
    },
};

#[derive(Debug)]
pub struct Null {
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
}

impl TryFrom<&Url> for Null {
    type Error = NexusBdevError;

    fn try_from(uri: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(uri);
        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: uri.to_string(),
                message: "no path segments".to_string(),
            });
        }

        let mut parameters: HashMap<String, String> =
            uri.query_pairs().into_owned().collect();

        let blk_size: u32 = if let Some(value) = parameters.remove("blk_size") {
            value.parse().context(nexus_uri::IntParamParseError {
                uri: uri.to_string(),
                parameter: String::from("blk_size"),
                value: value.clone(),
            })?
        } else {
            512
        };

        if blk_size != 512 && blk_size != 4096 {
            return Err(NexusBdevError::UriInvalid {
                uri: uri.to_string(),
                message:
                    "invalid blk_size specified must be one of 512 or 4096"
                        .to_string(),
            });
        }

        let size: u32 = if let Some(value) = parameters.remove("size_mb") {
            value.parse().context(nexus_uri::IntParamParseError {
                uri: uri.to_string(),
                parameter: String::from("size_mb"),
                value: value.clone(),
            })?
        } else {
            0
        };

        let num_blocks: u32 =
            if let Some(value) = parameters.remove("num_blocks") {
                value.parse().context(nexus_uri::IntParamParseError {
                    uri: uri.to_string(),
                    parameter: String::from("blk_size"),
                    value: value.clone(),
                })?
            } else {
                0
            };

        if size != 0 && num_blocks != 0 {
            return Err(NexusBdevError::UriInvalid {
                uri: uri.to_string(),
                message: "conflicting parameters num_blocks and size_mb are mutually exclusive"
                    .to_string(),
            });
        }

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: uri.to_string(),
            },
        )?;

        reject_unknown_parameters(uri, parameters)?;

        Ok(Self {
            name: uri.path()[1 ..].into(),
            alias: uri.to_string(),
            num_blocks: if num_blocks != 0 {
                num_blocks
            } else {
                (size << 20) / blk_size
            } as u64,
            blk_size,
            uuid: uuid.or_else(|| Some(Uuid::new_v4())),
        })
    }
}

impl GetName for Null {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Null {
    type Error = NexusBdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        if UntypedBdev::lookup_by_name(&self.name).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.name.clone(),
            });
        }

        let cname = self.name.clone().into_cstring();

        let opts = spdk_rs::libspdk::spdk_null_bdev_opts {
            name: cname.as_ptr(),
            uuid: std::ptr::null(),
            num_blocks: self.num_blocks,
            block_size: self.blk_size,
            md_size: 0,
            md_interleave: false,
            dif_type: spdk_rs::libspdk::SPDK_DIF_DISABLE,
            dif_is_head_of_md: false,
        };

        let errno = unsafe {
            let mut bdev: *mut spdk_rs::libspdk::spdk_bdev =
                std::ptr::null_mut();
            spdk_rs::libspdk::bdev_null_create(&mut bdev, &opts)
        };

        if errno != 0 {
            return Err(NexusBdevError::CreateBdev {
                source: Errno::from_i32(errno.abs()),
                name: self.name.clone(),
            });
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

        Err(NexusBdevError::BdevNotFound {
            name: self.name.clone(),
        })
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.name) {
            bdev.remove_alias(&self.alias);
            let (s, r) = oneshot::channel::<ErrnoResult<()>>();
            unsafe {
                spdk_rs::libspdk::bdev_null_delete(
                    bdev.unsafe_inner_mut_ptr(),
                    Some(done_errno_cb),
                    cb_arg(s),
                )
            };

            r.await
                .context(nexus_uri::CancelBdev {
                    name: self.name.clone(),
                })?
                .context(nexus_uri::DestroyBdev {
                    name: self.name,
                })
        } else {
            Err(NexusBdevError::BdevNotFound {
                name: self.name,
            })
        }
    }
}
