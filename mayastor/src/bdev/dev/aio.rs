use std::{collections::HashMap, convert::TryFrom, ffi::CString};

use async_trait::async_trait;
use futures::channel::oneshot;
use snafu::ResultExt;
use url::Url;

use spdk_sys::{bdev_aio_delete, create_aio_bdev};

use crate::{
    bdev::{util::uri, CreateDestroy, GetName},
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, NexusBdevError},
};

#[derive(Debug)]
pub(super) struct Aio {
    name: String,
    file: String,
    blk_size: u32,
}

/// Convert a URI to an Aio "object"
impl TryFrom<&Url> for Aio {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("no path segments"),
            });
        }

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let blk_size: u32 = match parameters.remove("blk_size") {
            Some(value) => {
                value.parse().context(nexus_uri::IntParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("blk_size"),
                })?
            }
            None => 512,
        };

        if let Some(keys) = uri::keys(parameters) {
            warn!("ignored parameters: {}", keys);
        }

        Ok(Aio {
            name: url.to_string(),
            file: format!("/{}", segments.join("/")),
            blk_size,
        })
    }
}

impl GetName for Aio {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait]
impl CreateDestroy for Aio {
    type Error = NexusBdevError;

    /// Create an AIO bdev
    async fn create(&self) -> Result<String, Self::Error> {
        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.get_name(),
            });
        }

        let cname = CString::new(self.get_name()).unwrap();
        let filename = CString::new(self.file.clone()).unwrap();

        let errno = unsafe {
            create_aio_bdev(cname.as_ptr(), filename.as_ptr(), self.blk_size)
        };

        async {
            errno_result_from_i32(self.get_name(), errno).context(
                nexus_uri::InvalidParams {
                    name: self.get_name(),
                },
            )
        }
        .await
    }

    /// Destroy the given AIO bdev
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        match Bdev::lookup_by_name(&self.name) {
            Some(bdev) => {
                let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
                unsafe {
                    bdev_aio_delete(
                        bdev.as_ptr(),
                        Some(done_errno_cb),
                        cb_arg(sender),
                    );
                }
                receiver
                    .await
                    .context(nexus_uri::CancelBdev {
                        name: self.get_name(),
                    })?
                    .context(nexus_uri::DestroyBdev {
                        name: self.get_name(),
                    })
            }
            None => Err(NexusBdevError::BdevNotFound {
                name: self.get_name(),
            }),
        }
    }
}
