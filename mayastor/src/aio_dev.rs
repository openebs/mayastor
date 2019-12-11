use crate::{
    bdev::bdev_lookup_by_name,
    executor::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, BdevError},
};
use futures::channel::oneshot;
use snafu::{ResultExt, Snafu};
use spdk_sys::{bdev_aio_delete, create_aio_bdev};
use std::{convert::TryFrom, ffi::CString};
use url::Url;

#[derive(Debug, Snafu)]
pub enum ParseError {
    #[snafu(display("Missing path to aio device"))]
    PathMissing {},
    #[snafu(display("Block size is not a number"))]
    BlockSizeInvalid {
        source: <u32 as std::str::FromStr>::Err,
    },
}

#[derive(Default, Clone, Debug)]
pub struct AioBdev {
    pub name: String,
    pub file: String,
    pub blk_size: u32,
}

// TODO: we cant use a trait as it does not support async yet
impl AioBdev {
    /// create an AIO bdev. The reason this is async is to avoid type errors
    /// when creating things concurrently.
    pub async fn create(self) -> Result<String, BdevError> {
        if bdev_lookup_by_name(&self.name).is_some() {
            return Err(BdevError::BdevExists {
                name: self.name.clone(),
            });
        }

        let cname = CString::new(self.name.clone()).unwrap();
        let filename = CString::new(self.file).unwrap();

        let errno = unsafe {
            create_aio_bdev(cname.as_ptr(), filename.as_ptr(), self.blk_size)
        };
        let name = self.name.clone();

        async {
            errno_result_from_i32(name.clone(), errno).context(
                nexus_uri::InvalidParams {
                    name,
                },
            )
        }
        .await
    }

    /// destroy the given aio bdev
    pub async fn destroy(self, bdev_name: &str) -> Result<(), BdevError> {
        if let Some(bdev) = bdev_lookup_by_name(bdev_name) {
            let (s, r) = oneshot::channel::<ErrnoResult<()>>();
            unsafe {
                bdev_aio_delete(bdev.as_ptr(), Some(done_errno_cb), cb_arg(s));
            }
            r.await.expect("Cancellation is not supported").context(
                nexus_uri::DestroyBdev {
                    name: self.name.clone(),
                },
            )
        } else {
            Err(BdevError::BdevNotFound {
                name: self.name.clone(),
            })
        }
    }
}

/// Converts an aio url to AioArgs
impl TryFrom<&Url> for AioBdev {
    type Error = ParseError;

    fn try_from(u: &Url) -> std::result::Result<Self, Self::Error> {
        let mut n = AioBdev::default();
        n.name = u.to_string();
        n.file = match u
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            None => return Err(ParseError::PathMissing {}),
            Some(s) => format!("/{}", s.join("/")),
        };
        n.blk_size = 0;

        let qp = u.query_pairs();
        for i in qp {
            if let "blk_size" = i.0.as_ref() {
                n.blk_size = i.1.parse().context(BlockSizeInvalid {})?;
                break;
            } else {
                warn!("query parameter {} ignored", i.0);
            }
        }
        Ok(n)
    }
}
