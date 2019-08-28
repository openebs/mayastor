use crate::{
    bdev::{bdev_lookup_by_name, nexus},
    executor::{cb_arg, complete_callback_1},
    nexus_uri::UriError,
};
use futures::{channel::oneshot, future};
use spdk_sys::{create_aio_bdev, delete_aio_bdev};
use std::{convert::TryFrom, ffi::CString};
use url::Url;

#[derive(Default, Debug)]
pub struct AioBdev {
    pub(crate) name: String,
    pub(crate) file: String,
    pub(crate) blk_size: u32,
}

// XXX we cant use a trait as it does not support async yet
impl AioBdev {
    /// create an AIO bdev. The reason this is async is to avoid type errors
    /// when creating things concurrently.
    pub async fn create(self) -> Result<String, nexus::Error> {
        if crate::bdev::bdev_lookup_by_name(&self.name).is_some() {
            info!("A bdev with name already exists {} exists", self.name);
            return Err(nexus::Error::ChildExists);
        }

        let cname = CString::new(self.name.clone()).unwrap();
        let filename = CString::new(self.file).unwrap();

        let rc = unsafe {
            create_aio_bdev(cname.as_ptr(), filename.as_ptr(), self.blk_size)
        };

        let fut = if rc == 0 {
            future::ok(self.name)
        } else {
            // upstream reports a better error, integrate that.
            future::err(nexus::Error::CreateFailed)
        };

        fut.await
    }

    /// destroy the given aio bdev
    pub async fn destroy(self) -> Result<(), nexus::Error> {
        type AioT = i32;
        if let Some(bdev) = bdev_lookup_by_name(&self.name) {
            let (s, r) = oneshot::channel::<AioT>();
            unsafe {
                delete_aio_bdev(
                    bdev.as_ptr(),
                    Some(complete_callback_1),
                    cb_arg(s),
                )
            };
            if r.await.unwrap() != 0 {
                Err(nexus::Error::Internal)
            } else {
                Ok(())
            }
        } else {
            Err(nexus::Error::NotFound)
        }
    }
}
/// Converts an aio url to AioArgs
impl TryFrom<&Url> for AioBdev {
    type Error = UriError;
    fn try_from(u: &Url) -> Result<Self, Self::Error> {
        let mut n = AioBdev::default();
        n.name = u.to_string();
        n.file = match u
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            None => return Err(UriError::InvalidPathSegment),
            Some(s) => format!("/{}", s.join("/").to_string()),
        };
        n.blk_size = 0;

        let qp = u.query_pairs();
        for i in qp {
            if let "blk_size" = i.0.as_ref() {
                n.blk_size = i.1.parse().unwrap()
            }
        }
        Ok(n)
    }
}
