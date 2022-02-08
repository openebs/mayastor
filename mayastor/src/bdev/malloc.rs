//!
//! The malloc bdev as the name implies, creates an in memory disk. Note
//! that the backing memory is allocated from huge pages and not from the
//! heap. IOW, you must ensure you do not run out of huge pages while using
//! this.
use std::{collections::HashMap, convert::TryFrom};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;
use url::Url;

use spdk_rs::{
    libspdk::{create_malloc_disk, delete_malloc_disk, spdk_bdev},
    DummyBdev,
};

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult, IntoCString},
    nexus_uri::{self, NexusBdevError},
};

#[derive(Debug)]
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
}

impl TryFrom<&Url> for Malloc {
    type Error = NexusBdevError;

    fn try_from(uri: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(uri);
        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: uri.to_string(),
                message: "empty path".to_string(),
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
                    parameter: String::from("num_blocks"),
                    value: value.clone(),
                })?
            } else {
                0
            };

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: uri.to_string(),
            },
        )?;

        reject_unknown_parameters(uri, parameters)?;

        // Validate parameters.
        if blk_size != 512 && blk_size != 4096 {
            return Err(NexusBdevError::UriInvalid {
                uri: uri.to_string(),
                message: "'blk_size' must be one of: 512, 4096".to_string(),
            });
        }

        if size != 0 && num_blocks != 0 {
            return Err(NexusBdevError::UriInvalid {
                uri: uri.to_string(),
                message: "'num_blocks' and 'size_mb' are mutually exclusive"
                    .to_string(),
            });
        }

        if size == 0 && num_blocks == 0 {
            return Err(NexusBdevError::UriInvalid {
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
    type Error = NexusBdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        if DummyBdev::lookup_by_name(&self.name).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.name.clone(),
            });
        }

        let cname = self.name.clone().into_cstring();

        let errno = unsafe {
            let mut bdev: *mut spdk_bdev = std::ptr::null_mut();
            create_malloc_disk(
                &mut bdev,
                cname.as_ptr(),
                std::ptr::null_mut(),
                self.num_blocks,
                self.blk_size,
                0,
            )
        };

        if errno != 0 {
            return Err(NexusBdevError::CreateBdev {
                source: Errno::from_i32(errno.abs()),
                name: self.name.clone(),
            });
        }

        if let Some(mut bdev) = DummyBdev::lookup_by_name(&self.name) {
            if let Some(uuid) = self.uuid {
                unsafe { bdev.set_uuid(uuid.into()) };
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
        if let Some(mut bdev) = DummyBdev::lookup_by_name(&self.name) {
            bdev.remove_alias(&self.alias);
            let (s, r) = oneshot::channel::<ErrnoResult<()>>();

            unsafe {
                delete_malloc_disk(
                    bdev.legacy_as_ptr().as_ptr(),
                    Some(done_errno_cb),
                    cb_arg(s),
                );
            }

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
