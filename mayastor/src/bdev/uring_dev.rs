use std::{convert::TryFrom, ffi::CString};

use futures::channel::oneshot;
use snafu::{ResultExt, Snafu};
use url::Url;

use spdk_sys::{create_uring_bdev, delete_uring_bdev};

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, BdevCreateDestroy},
};

#[derive(Debug, Snafu)]
pub enum UringParseError {
    #[snafu(display("Missing path to io_uring device"))]
    PathMissing {},
    #[snafu(display("Block size is not a number"))]
    BlockSizeInvalid {
        source: <u32 as std::str::FromStr>::Err,
    },
}

#[derive(Default, Clone, Debug)]
pub struct UringBdev {
    pub name: String,
    pub file: String,
    pub blk_size: u32,
}

// TODO: we can't use a trait as it does not support async yet
impl UringBdev {
    /// create a uring bdev. The reason this is async is to avoid type errors
    /// when creating things concurrently.
    pub async fn create(self) -> Result<String, BdevCreateDestroy> {
        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevCreateDestroy::BdevExists {
                name: self.name.clone(),
            });
        }

        let cname = CString::new(self.name.clone()).unwrap();
        let filename = CString::new(self.file).unwrap();

        let spdk_bdev_ptr = unsafe {
            create_uring_bdev(cname.as_ptr(), filename.as_ptr(), self.blk_size)
        };
        let name = self.name.clone();

        async {
            if !spdk_bdev_ptr.is_null() {
                Ok(name)
            } else {
                errno_result_from_i32(name.clone(), -1)
                    .context(nexus_uri::InvalidParams { name })
            }
        }
        .await
    }

    /// destroy the given uring bdev
    pub async fn destroy(self) -> Result<(), BdevCreateDestroy> {
        if let Some(bdev) = Bdev::lookup_by_name(&self.name) {
            let (s, r) = oneshot::channel::<ErrnoResult<()>>();
            unsafe {
                delete_uring_bdev(
                    bdev.as_ptr(),
                    Some(done_errno_cb),
                    cb_arg(s),
                );
            }
            r.await.expect("Cancellation is not supported").context(
                nexus_uri::DestroyBdev {
                    name: self.name.clone(),
                },
            )
        } else {
            Err(BdevCreateDestroy::BdevNotFound {
                name: self.name.clone(),
            })
        }
    }
}

/// Converts an uring url to UringArgs
impl TryFrom<&Url> for UringBdev {
    type Error = UringParseError;

    fn try_from(u: &Url) -> std::result::Result<Self, Self::Error> {
        let mut n = UringBdev::default();
        n.name = u.to_string();
        n.file = match u
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            None => return Err(UringParseError::PathMissing {}),
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
