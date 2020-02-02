use std::{convert::TryFrom, ffi::CString, os::raw::c_void};

use futures::channel::oneshot;
use snafu::{ResultExt, Snafu};
use url::Url;

use spdk_sys::{create_iscsi_disk, delete_iscsi_disk, spdk_bdev};

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, BdevCreateDestroy},
};

#[derive(Debug, Snafu)]
pub enum IscsiParseError {
    // no parse errors for iscsi urls - we should have some probably
}

#[derive(Default, Debug)]
pub struct IscsiBdev {
    pub(crate) name: String,
    pub(crate) iqn: String,
    pub(crate) url: String,
}

impl IscsiBdev {
    /// create an iscsi target
    pub async fn create(self) -> Result<String, BdevCreateDestroy> {
        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevCreateDestroy::BdevExists {
                name: self.name.clone(),
            });
        }

        extern "C" fn wrap(arg: *mut c_void, bdev: *mut spdk_bdev, errno: i32) {
            let sender = unsafe {
                Box::from_raw(
                    arg as *const _
                        as *mut oneshot::Sender<ErrnoResult<*mut spdk_bdev>>,
                )
            };

            sender
                .send(errno_result_from_i32(bdev, errno))
                .expect("Receiver is gone");
        }

        let cname = CString::new(self.name.clone()).unwrap();
        let cinitiator = CString::new(self.iqn.clone()).unwrap();
        let curl = CString::new(self.url.clone()).unwrap();
        let (s, r) = oneshot::channel::<ErrnoResult<*mut spdk_bdev>>();
        let errno = unsafe {
            create_iscsi_disk(
                cname.as_ptr(),
                curl.as_ptr(),
                cinitiator.as_ptr(),
                Some(wrap),
                cb_arg(s),
            )
        };
        errno_result_from_i32((), errno).context(nexus_uri::InvalidParams {
            name: self.name.clone(),
        })?;

        let bdev_ptr = r
            .await
            .expect("Cancellation is not supported")
            .context(nexus_uri::CreateBdev {
                name: self.name.clone(),
            })?;

        Ok(Bdev::from(bdev_ptr).name())
    }

    // destroy the given bdev
    pub async fn destroy(self) -> Result<(), BdevCreateDestroy> {
        if let Some(bdev) = Bdev::lookup_by_name(&self.name) {
            let (s, r) = oneshot::channel::<ErrnoResult<()>>();
            unsafe {
                delete_iscsi_disk(
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
                name: self.name,
            })
        }
    }
}

/// Converts an iSCSI url to a struct iSCSiArgs. NOTE do to a bug in SPDK
/// providing a valid target with an invalid iqn, will crash the system.
impl TryFrom<&Url> for IscsiBdev {
    type Error = IscsiParseError;

    fn try_from(u: &Url) -> Result<Self, Self::Error> {
        let mut n = IscsiBdev::default();
        n.iqn = format!("iqn.1980-05.mayastor:{}", uuid::Uuid::new_v4());
        n.name = u.to_string();
        n.url = format!("{}/0", u.to_string());

        Ok(n)
    }
}
