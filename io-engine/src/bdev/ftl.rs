//! The Ftl device is a representation of an SPDK (CSAL) ftl bdev, which allows
//! to create a layered device with a persistent (fast) cache device for
//! buffering writes that get eventually flushed out sequentially to a base
//! device.
//!
//! # Uri
//! ftl:///$ftl_device_name?bbdev=$bbdev_uri_percent_encoded&
//! cbdev=$cbdev_uri_percent_encoded
//!
//! The bbdev_uri and cbdev_uri need to use percent encoding on '?' (= '%3F')
//! and '&' (= '%26') segment dividers. This is needed so we can successfully
//! parse the ftl uri. Optionally, the sub-uris may also be fully
//! percent-encoded.
//!
//! # Parameters
//! name: A name for the ftl device, example: "ftl-1".
//! alias: Ftl device URI which can be used to open the bdev.
//! uuid: A UUID that can be set to reference the resulting SPDK bdev.
//! bbdev_uri: Nested device uri for the ftl base bdev, which is (partially)
//!            percent encoded. This device currently requires to be a
//!            non-volatile memory device which is has an LBA format of 4KiB
//!            and is at least 20GB large.
//! cbdev_uri: Nested device uri for the ftl base bdev, which is (partially)
//!            percent encoded. This device currently requires to be a
//!            non-volatile memory device which is has an LBA format of 4KiB
//!            data size, 64B metadata size and is at least 20GB large.
//!            From SPDK v24.09 on there is no requirement for the metadata
//!            portion as Variable Sector Size (VSS) emulation is enabled.
//!
//! # Examples
//! ftl:///ftl-1?bbdev=pcie:///0000:01:00.0&cbdev=pcie:///0000:02:00.0
//!
//! ftl:///ftl-1?bbdev=pcie%3A%2F%2F%2F0000%3A01%3A00.0&cbdev=pcie%3A%2F%2F%
//! 2F0000%3A02%3A00.0
//!
//! ftl:///ftl-1?bbdev=pcie:///0000:01:00.0%
//! 3Finvalidoption=test&cbdev=pcie:///0000:02:00.0
//!
//! ftl:///ftl-2?bbdev=aio:///tmp/basedev.img%3Fblk_size=4096&cbdev=aio:///tmp/
//! cachedev.img%3Fblk_size=4096
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Formatter},
    os::raw::c_char,
};

use core::ffi::c_void;

use async_trait::async_trait;
use futures::channel::oneshot;
use log::info;
use nix::errno::Errno;
use percent_encoding::percent_decode_str;
use snafu::{OptionExt, ResultExt};
use std::mem;
use url::Url;

use spdk_rs::{
    ffihelper::errno_result_from_i32,
    libspdk::{
        bdev_ftl_create_bdev, bdev_ftl_delete_bdev, ftl_bdev_info, spdk_ftl_conf,
        spdk_ftl_get_default_conf, spdk_ftl_mode,
    },
    UntypedBdev,
};

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    bdev_api::{self, bdev_create, bdev_destroy, BdevError},
    core::VerboseError,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
};

/// An ftl bdev specified via URI.
pub struct Ftl {
    /// The name of the ftl-bdev we created.
    name: String,
    /// Alias which can be used to open the bdev.
    alias: String,
    /// Uuid of the spdk bdev.
    uuid: Option<uuid::Uuid>,
    /// Ftl's base bdev URI.
    bbdev_uri: String,
    /// Ftl's cache bdev URI.
    cbdev_uri: String,
}

impl Debug for Ftl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Ftl '{}' (bbdev_uri {} cbdev_uri {})",
            self.name, self.bbdev_uri, self.cbdev_uri
        )
    }
}

impl TryFrom<&Url> for Ftl {
    type Error = BdevError;

    fn try_from(uri: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(uri);
        if segments.is_empty() {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "empty path".to_string(),
            });
        }

        let mut parameters: HashMap<String, String> = uri.query_pairs().into_owned().collect();

        let uuid =
            uri::uuid(parameters.remove("uuid")).context(bdev_api::UuidParamParseFailed {
                uri: uri.to_string(),
            })?;

        let encoded_bbdev_uri = parameters.remove("bbdev").context(bdev_api::InvalidUri {
            uri: uri.to_string(),
            message: String::from("No bbdev parameter found"),
        })?;

        let bbdev_uri = percent_decode_str(&encoded_bbdev_uri)
            .decode_utf8()
            .map_err(|e| BdevError::InvalidUri {
                uri: uri.to_string(),
                message: format!("Could not percent decode bbdev_uri sub-uri - {e}"),
            })?
            .to_string();

        let encoded_cbdev_uri = parameters.remove("cbdev").context(bdev_api::InvalidUri {
            uri: uri.to_string(),
            message: String::from("No cbdev parameter found"),
        })?;

        let cbdev_uri = percent_decode_str(&encoded_cbdev_uri)
            .decode_utf8()
            .map_err(|e| BdevError::InvalidUri {
                uri: uri.to_string(),
                message: format!("Could not percent decode cbdev_uri sub-uri - {e}"),
            })?
            .to_string();

        // Device parameter checking for bbdev and cbdev are done in
        // bdev_ftl_create_bdev
        reject_unknown_parameters(uri, parameters)?;

        Ok(Self {
            name: uri.path()[1..].into(),
            alias: uri.to_string(),
            uuid,
            bbdev_uri,
            cbdev_uri,
        })
    }
}

impl GetName for Ftl {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}
impl super::Probe for Ftl {}

pub extern "C" fn ftl_bdev_init_fn_cb(
    _ptr: *const ftl_bdev_info,
    sender_ptr: *mut c_void,
    errno: i32,
) {
    info!("{:?}: ftl_bdev_init_fn_cb", errno);
    let sender = unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<ErrnoResult<()>>) };
    sender
        .send(errno_result_from_i32((), errno))
        .expect("done callback receiver side disappeared");
}

#[async_trait(?Send)]
impl CreateDestroy for Ftl {
    type Error = BdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        if UntypedBdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevError::BdevExists {
                name: self.name.clone(),
            });
        }

        let ftl_dev_name = CString::new(self.name.clone()).unwrap();

        debug!(
            "{:?}: Creating ftl-bdev '{:?}' with bbdev {:?} and cbdev {:?}.",
            self, ftl_dev_name, self.bbdev_uri, self.cbdev_uri
        );

        use std::ffi::CString;

        let base_dev_name = bdev_create(&self.bbdev_uri).await?;
        let cache_dev_name = match bdev_create(&self.cbdev_uri).await {
            Ok(dev_name) => dev_name,
            Err(err) => {
                if let Err(e) = bdev_destroy(&self.bbdev_uri).await {
                    error!("{:?} bbdev cleanup error: {}", self, e);
                }
                return Err(err);
            }
        };

        let (s, r) = oneshot::channel::<ErrnoResult<()>>();

        let spdk_ftl_conf_size = mem::size_of::<spdk_ftl_conf>() as u64;
        let mut ftl_conf = spdk_ftl_conf {
            ..unsafe { mem::zeroed() }
        };
        unsafe { spdk_ftl_get_default_conf(&mut ftl_conf, spdk_ftl_conf_size) };
        ftl_conf.name = ftl_dev_name.as_ptr() as *mut c_char;
        ftl_conf.base_bdev = base_dev_name.as_ptr() as *mut c_char;
        ftl_conf.cache_bdev = cache_dev_name.as_ptr() as *mut c_char;
        ftl_conf.fast_shutdown = true;
        ftl_conf.verbose_mode = true;
        ftl_conf.mode = spdk_ftl_mode::SPDK_FTL_MODE_CREATE as u32;

        let errno =
            unsafe { bdev_ftl_create_bdev(&ftl_conf, Some(ftl_bdev_init_fn_cb), cb_arg(s)) };

        if errno != 0 {
            let err = BdevError::CreateBdevFailed {
                source: Errno::from_raw(errno.abs()),
                name: self.name.clone(),
            };

            error!("{:?} error: {}", self, err.verbose());

            if let Err(e) = bdev_destroy(&self.bbdev_uri).await {
                error!("{:?} bbdev cleanup error: {}", self, e);
            }
            if let Err(e) = bdev_destroy(&self.cbdev_uri).await {
                error!("{:?} cbdev cleanup error: {}", self, e);
            }

            return Err(err);
        }

        r.await
            .context(bdev_api::BdevCommandCanceled {
                name: self.name.clone(),
            })?
            .context(bdev_api::CreateBdevFailed {
                name: self.name.clone(),
            })?;

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

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        debug!("{:?}: deleting", self);

        let Some(bdev) = UntypedBdev::lookup_by_name(&self.name) else {
            return Err(BdevError::BdevNotFound { name: self.name });
        };

        let (s, r) = oneshot::channel::<ErrnoResult<()>>();

        unsafe {
            bdev_ftl_delete_bdev(
                (*bdev.unsafe_inner_ptr()).name,
                true,
                Some(done_errno_cb),
                cb_arg(s),
            );
        }

        r.await
            .context(bdev_api::BdevCommandCanceled {
                name: self.name.clone(),
            })?
            .context(bdev_api::DestroyBdevFailed { name: self.name })?;

        let mut result = bdev_destroy(&self.bbdev_uri).await;

        if let Err(e) = bdev_destroy(&self.cbdev_uri).await {
            if result.is_ok() {
                result = Err(e);
            }
        }
        result
    }
}
