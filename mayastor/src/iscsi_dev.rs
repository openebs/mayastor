use crate::{
    bdev::{bdev_lookup_by_name, nexus::Error},
    executor::{cb_arg, complete_callback_1},
    nexus_uri::UriError,
};
use futures::channel::oneshot;
use spdk_sys::{create_iscsi_disk, delete_iscsi_disk, spdk_bdev};
use std::{convert::TryFrom, ffi::CString, os::raw::c_void};
use url::Url;

#[derive(Default, Debug)]
pub struct IscsiBdev {
    pub(crate) name: String,
    pub(crate) iqn: String,
    pub(crate) url: String,
}
impl IscsiBdev {
    /// create an iscsi target
    pub async fn create(self) -> Result<String, Error> {
        if crate::bdev::bdev_lookup_by_name(&self.name).is_some() {
            info!("bdev with name {} exists", self.name);
            return Err(Error::ChildExists);
        }

        extern "C" fn wrap(
            arg: *mut c_void,
            _bdev: *mut spdk_bdev,
            status: i32,
        ) {
            let sender = unsafe {
                Box::from_raw(arg as *const _ as *mut oneshot::Sender<i32>)
            };

            sender
                .send(status)
                .expect("failed to execute iscsi create callback");
        }

        let cname = CString::new(self.name.clone())?;
        let cinitiator = CString::new(self.iqn)?;
        let curl = CString::new(self.url)?;

        let (s, r) = oneshot::channel::<i32>();

        let mut ret = unsafe {
            create_iscsi_disk(
                cname.as_ptr(),
                curl.as_ptr(),
                cinitiator.as_ptr(),
                Some(wrap),
                cb_arg(s),
            )
        };

        if ret != 0 {
            return Err(Error::Internal(
                "Failed to create iscsi bdev".to_owned(),
            ));
        }

        ret = r.await.expect("completion failure for iscsi create");

        if ret != 0 {
            Err(Error::CreateFailed)
        } else {
            Ok(self.name)
        }
    }

    // destroy the given bdev
    pub async fn destroy(self) -> Result<(), Error> {
        type CbT = i32;

        if let Some(bdev) = bdev_lookup_by_name(&self.name) {
            let (s, r) = oneshot::channel::<CbT>();
            unsafe {
                delete_iscsi_disk(
                    bdev.inner,
                    Some(complete_callback_1),
                    cb_arg(s),
                )
            };
            if r.await.unwrap() != 0 {
                Err(Error::CreateFailed)
            } else {
                Ok(())
            }
        } else {
            Err(Error::Exists)
        }
    }
}
/// Converts an iSCSI url to a struct iSCSiArgs. NOTE do to a bug in SPDK
/// providing a valid target with an invalid iqn, will crash the system.
impl TryFrom<&Url> for IscsiBdev {
    type Error = UriError;
    fn try_from(u: &Url) -> Result<Self, Self::Error> {
        let mut n = IscsiBdev::default();
        n.iqn = format!("iqn.1980-05.mayastor:{}", uuid::Uuid::new_v4());
        n.name = u.to_string();
        n.url = format!("{}/0", u.to_string());

        Ok(n)
    }
}

/// create an iscsi target
pub async fn iscsi_create(args: IscsiBdev) -> Result<String, i32> {
    if crate::bdev::bdev_lookup_by_name(&args.name).is_some() {
        info!("bdev with name {} exists", args.name);
        return Err(-1);
    }

    extern "C" fn wrap(arg: *mut c_void, bdev: *mut spdk_bdev, status: i32) {
        let sender = unsafe {
            Box::from_raw(
                arg as *const _ as *mut oneshot::Sender<Result<String, i32>>,
            )
        };

        if status != 0 {
            sender.send(Err(status)).unwrap();
        } else {
            sender
                .send(Ok(crate::bdev::Bdev::from(bdev).name()))
                .unwrap();
        }
    }

    let cname = CString::new(args.name).unwrap();
    let cinitiator = CString::new(args.iqn).unwrap();
    let curl = CString::new(args.url).unwrap();

    let (s, r) = oneshot::channel::<Result<String, i32>>();

    let _ret = unsafe {
        create_iscsi_disk(
            cname.as_ptr(),
            curl.as_ptr(),
            cinitiator.as_ptr(),
            Some(wrap),
            cb_arg(s),
        )
    };

    r.await.unwrap()
}

/// destroy the given bdev
#[allow(clippy::needless_lifetimes)]
pub async fn iscsi_destroy(name: &str) -> Result<(), ()> {
    type CbT = i32;

    if let Some(bdev) = bdev_lookup_by_name(name) {
        let (s, r) = oneshot::channel::<CbT>();
        unsafe {
            delete_iscsi_disk(bdev.inner, Some(complete_callback_1), cb_arg(s))
        };
        if r.await.unwrap() != 0 {
            Err(())
        } else {
            Ok(())
        }
    } else {
        Err(())
    }
}
