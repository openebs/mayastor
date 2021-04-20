use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::CString,
    os::raw::{c_int, c_void},
};

use async_trait::async_trait;
use futures::channel::oneshot;
use snafu::ResultExt;
use url::Url;
use uuid::Uuid;

use spdk_sys::{create_iscsi_disk, delete_iscsi_disk, spdk_bdev};

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, NexusBdevError},
};

const ISCSI_IQN_PREFIX: &str = "iqn.1980-05.mayastor";

#[derive(Debug)]
pub(super) struct Iscsi {
    name: String,
    alias: String,
    iqn: String,
    url: String,
    uuid: Option<uuid::Uuid>,
}

/// Convert a URI to an Iscsi "object"
/// NOTE: due to a bug in SPDK, providing a valid
/// target with an invalid iqn will crash the system.
impl TryFrom<&Url> for Iscsi {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        if url.host_str().is_none() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("missing host"),
            });
        }

        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("no path segment"),
            });
        }

        if segments.len() > 2 {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("too many path segments"),
            });
        }

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: url.to_string(),
            },
        )?;

        reject_unknown_parameters(url, parameters)?;

        Ok(Iscsi {
            name: url[url::Position::BeforeHost .. url::Position::AfterPath]
                .into(),
            alias: url.to_string(),
            iqn: format!("{}:{}", ISCSI_IQN_PREFIX, Uuid::new_v4()),
            url: if segments.len() == 2 {
                url[.. url::Position::AfterPath].to_string()
            } else {
                format!("{}/0", &url[.. url::Position::AfterPath])
            },
            uuid,
        })
    }
}

impl GetName for Iscsi {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Iscsi {
    type Error = NexusBdevError;

    /// Create an iSCSI bdev
    async fn create(&self) -> Result<String, Self::Error> {
        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.get_name(),
            });
        }

        extern "C" fn done_iscsi_create_cb(
            arg: *mut c_void,
            bdev: *mut spdk_bdev,
            errno: c_int,
        ) {
            let sender = unsafe {
                Box::from_raw(
                    arg as *mut oneshot::Sender<ErrnoResult<*mut spdk_bdev>>,
                )
            };

            sender
                .send(errno_result_from_i32(bdev, errno))
                .expect("done callback receiver side disappeared");
        }

        let cname = CString::new(self.get_name()).unwrap();
        let curl = CString::new(self.url.clone()).unwrap();
        let cinitiator = CString::new(self.iqn.clone()).unwrap();

        let (sender, receiver) = oneshot::channel::<ErrnoResult<Bdev>>();

        let errno = unsafe {
            create_iscsi_disk(
                cname.as_ptr(),
                curl.as_ptr(),
                cinitiator.as_ptr(),
                Some(done_iscsi_create_cb),
                cb_arg(sender),
            )
        };

        errno_result_from_i32((), errno).context(nexus_uri::InvalidParams {
            name: self.get_name(),
        })?;

        let mut bdev = receiver
            .await
            .context(nexus_uri::CancelBdev {
                name: self.get_name(),
            })?
            .context(nexus_uri::CreateBdev {
                name: self.get_name(),
            })?;

        if let Some(u) = self.uuid {
            bdev.set_uuid(u);
        }
        if !bdev.add_alias(&self.alias) {
            error!(
                "Failed to add alias {} to device {}",
                self.alias,
                self.get_name()
            );
        }

        Ok(bdev.name())
    }

    /// Destroy the given iSCSI bdev
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        match Bdev::lookup_by_name(&self.name) {
            Some(bdev) => {
                let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
                unsafe {
                    delete_iscsi_disk(
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
