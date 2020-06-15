use std::{
    ffi::{c_void, CString},
    fmt::Debug,
    mem::size_of,
    ptr,
};

use futures::channel::oneshot;
use serde::export::{Formatter, TryFrom};

use crate::{
    core::{Bdev, Reactors},
    ffihelper::{cb_arg, AsStr, IntoCString},
    subsys::{
        nvmf::{transport::TransportID, Error, NVMF_TGT},
        Config,
    },
};
use spdk_sys::{
    spdk_bdev_nvme_opts,
    spdk_nvmf_ns_get_bdev,
    spdk_nvmf_ns_opts,
    spdk_nvmf_subsystem,
    spdk_nvmf_subsystem_add_listener,
    spdk_nvmf_subsystem_add_ns,
    spdk_nvmf_subsystem_create,
    spdk_nvmf_subsystem_destroy,
    spdk_nvmf_subsystem_get_first,
    spdk_nvmf_subsystem_get_first_listener,
    spdk_nvmf_subsystem_get_first_ns,
    spdk_nvmf_subsystem_get_next,
    spdk_nvmf_subsystem_get_next_listener,
    spdk_nvmf_subsystem_get_nqn,
    spdk_nvmf_subsystem_listener_get_trid,
    spdk_nvmf_subsystem_pause,
    spdk_nvmf_subsystem_resume,
    spdk_nvmf_subsystem_set_allow_any_host,
    spdk_nvmf_subsystem_set_mn,
    spdk_nvmf_subsystem_set_sn,
    spdk_nvmf_subsystem_start,
    spdk_nvmf_subsystem_stop,
    SPDK_NVMF_SUBTYPE_NVME,
};
use tracing::instrument;

pub struct NvmfSubsystemIterator(*mut spdk_nvmf_subsystem);

impl Iterator for NvmfSubsystemIterator {
    type Item = NvmfSubsystem;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_null() {
            None
        } else {
            let current = self.0;
            self.0 = unsafe { spdk_nvmf_subsystem_get_next(current) };
            Some(NvmfSubsystem::from(current))
        }
    }
}
pub struct NvmfSubsystem(*mut spdk_nvmf_subsystem);

impl IntoIterator for NvmfSubsystem {
    type Item = NvmfSubsystem;
    type IntoIter = NvmfSubsystemIterator;

    fn into_iter(self) -> Self::IntoIter {
        NVMF_TGT.with(|t| {
            NvmfSubsystemIterator(unsafe {
                spdk_nvmf_subsystem_get_first(t.borrow().tgt_as_ptr())
            })
        })
    }
}

impl Debug for NvmfSubsystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        if let Some(e) = self.uri_endpoints() {
            write!(f, "{:?}", e).unwrap();
        }
        Ok(())
    }
}

impl From<*mut spdk_nvmf_subsystem> for NvmfSubsystem {
    fn from(s: *mut spdk_nvmf_subsystem) -> Self {
        NvmfSubsystem(s)
    }
}

impl TryFrom<&Bdev> for NvmfSubsystem {
    type Error = Error;

    fn try_from(bdev: &Bdev) -> Result<Self, Self::Error> {
        let ss = NvmfSubsystem::new(bdev.name().as_str())?;
        ss.allow_any(true);
        ss.add_namespace(bdev)?;
        Ok(ss)
    }
}

impl NvmfSubsystem {
    #[instrument]
    pub fn new(uuid: &str) -> Result<Self, Error> {
        let nqn = gen_nqn(uuid).into_cstring();
        let ss = NVMF_TGT.with(|t| {
            let tgt = t.borrow().tgt_as_ptr();
            unsafe {
                spdk_nvmf_subsystem_create(
                    tgt,
                    nqn.as_ptr(),
                    SPDK_NVMF_SUBTYPE_NVME,
                    1,
                )
            }
        });

        if ss.is_null() {
            Err(Error::Subsystem {
                nqn: uuid.into(),
                msg: "ss ptr is null".into(),
            })
        } else {
            let sn = CString::new("33' ~'~._`o##o>").unwrap();
            if unsafe { spdk_nvmf_subsystem_set_sn(ss, sn.as_ptr()) } != 0 {
                return Err(Error::Subsystem {
                    nqn: uuid.into(),
                    msg: "failed to set serial".into(),
                });
            }
            let mn = CString::new("Mayastor NVMe controller").unwrap();
            if unsafe { spdk_nvmf_subsystem_set_mn(ss, mn.as_ptr()) } != 0 {
                return Err(Error::Subsystem {
                    nqn: uuid.into(),
                    msg: "failed to set serial".into(),
                });
            }

            Ok(Self(ss))
        }
    }

    pub fn new_with_uuid(uuid: &str, bdev: &Bdev) -> Result<Self, Error> {
        let ss = NvmfSubsystem::new(uuid)?;
        ss.allow_any(true);
        ss.add_namespace(bdev)?;
        Ok(ss)
    }

    #[instrument(debug)]
    pub fn add_namespace(&self, bdev: &Bdev) -> Result<(), Error> {
        let mut opts = spdk_nvmf_ns_opts::default();
        opts.nguid = bdev.uuid().as_bytes();
        let ns_id = unsafe {
            spdk_nvmf_subsystem_add_ns(
                self.0,
                bdev.as_ptr(),
                &opts as *const _,
                size_of::<spdk_bdev_nvme_opts>() as u64,
                ptr::null_mut(),
            )
        };
        if ns_id == 0 {
            Err(Error::Namespace {
                bdev: bdev.clone(),
                msg: "failed to add namespace ID".to_string(),
            })
        } else {
            Ok(())
        }
    }

    pub fn destroy(&self) {
        unsafe { spdk_nvmf_subsystem_destroy(self.0) }
    }

    /// Get NVMe subsystem's NQN
    pub fn get_nqn(&self) -> String {
        unsafe { spdk_nvmf_subsystem_get_nqn(self.0).as_str().to_string() }
    }

    pub fn allow_any(&self, enable: bool) {
        unsafe { spdk_nvmf_subsystem_set_allow_any_host(self.0, enable) };
    }

    // we currently allow all listeners so no need ..
    async fn add_listner(&self) -> Result<(), Error> {
        extern "C" fn listen_cb(arg: *mut c_void, status: i32) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
            s.send(status).unwrap();
        }

        let cfg = Config::by_ref();
        // let mut trid_nexus =
        //     TransportID::new(cfg.nexus_opts.nvmf_nexus_port).unwrap();
        let trid_replica = TransportID::new(cfg.nexus_opts.nvmf_replica_port);

        let (s, r) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_subsystem_add_listener(
                self.0,
                trid_replica.as_ptr(),
                Some(listen_cb),
                cb_arg(s),
            );
        }
        r.await.unwrap();

        // let (s, r) = oneshot::channel::<i32>();
        // unsafe {
        //     spdk_nvmf_subsystem_add_listener(
        //         self.0,
        //         &mut trid_nexus as *mut _,
        //         Some(listen_cb),
        //         cb_arg(s),
        //     );
        // }
        //
        // r.await.unwrap();

        Ok(())
    }
    #[instrument(debug)]
    pub async fn start(self) -> Result<(), Error> {
        extern "C" fn start_cb(
            ss: *mut spdk_nvmf_subsystem,
            arg: *mut c_void,
            status: i32,
        ) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
            // tracing::debug!("this is a tracing line");
            let ss = NvmfSubsystem::from(ss);
            if status != 0 {
                error!(
                    "Failed start subsystem state {} -- destroying it",
                    ss.get_nqn()
                );
                ss.destroy();
            }

            s.send(status).unwrap();
        }

        self.add_listner().await?;

        let (s, r) = oneshot::channel::<i32>();

        let mut rc = unsafe {
            spdk_nvmf_subsystem_start(self.0, Some(start_cb), cb_arg(s))
        };

        if rc != 0 {
            return Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "out of memory".to_string(),
            });
        }

        rc = r.await.unwrap();
        if rc != 0 {
            Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "failed to start the subsystem".to_string(),
            })
        } else {
            info!("{:?} started", self);
            Ok(())
        }
    }

    #[instrument(debug)]
    pub async fn stop(&self) -> Result<(), Error> {
        extern "C" fn stop_cb(
            ss: *mut spdk_nvmf_subsystem,
            arg: *mut c_void,
            status: i32,
        ) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };

            let ss = NvmfSubsystem::from(ss);
            if status != 0 {
                error!(
                    "Failed change subsystem state {} -- to STOP",
                    ss.get_nqn()
                );
            }

            s.send(status).unwrap();
        }

        let (s, r) = oneshot::channel::<i32>();
        debug!("stopping {:?}", self);
        let mut rc = unsafe {
            spdk_nvmf_subsystem_stop(self.0, Some(stop_cb), cb_arg(s))
        };

        if rc != 0 {
            return Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "out of memory".to_string(),
            });
        }

        rc = r.await.unwrap();
        if rc != 0 {
            Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "failed to stop the subsystem".to_string(),
            })
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    async fn pause(&self) -> Result<(), Error> {
        extern "C" fn pause_cb(
            ss: *mut spdk_nvmf_subsystem,
            arg: *mut c_void,
            status: i32,
        ) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };

            let ss = NvmfSubsystem::from(ss);
            if status != 0 {
                error!(
                    "Failed change subsystem state {} -- to pause",
                    ss.get_nqn()
                );
            }

            s.send(status).unwrap();
        }

        let (s, r) = oneshot::channel::<i32>();

        let mut rc = unsafe {
            spdk_nvmf_subsystem_pause(self.0, Some(pause_cb), cb_arg(s))
        };

        if rc != 0 {
            return Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "out of memory".to_string(),
            });
        }

        rc = r.await.unwrap();
        if rc != 0 {
            Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "failed to stop the subsystem".to_string(),
            })
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    async fn resume(&self) -> Result<(), Error> {
        extern "C" fn resume_cb(
            ss: *mut spdk_nvmf_subsystem,
            arg: *mut c_void,
            status: i32,
        ) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };

            let ss = NvmfSubsystem::from(ss);
            if status != 0 {
                error!(
                    "Failed change subsystem state {} -- to RESUME",
                    ss.get_nqn()
                );
            }

            s.send(status).unwrap();
        }

        let (s, r) = oneshot::channel::<i32>();

        let mut rc = unsafe {
            spdk_nvmf_subsystem_resume(self.0, Some(resume_cb), cb_arg(s))
        };

        if rc != 0 {
            return Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "out of memory".to_string(),
            });
        }

        rc = r.await.unwrap();
        if rc != 0 {
            Err(Error::Subsystem {
                nqn: self.get_nqn(),
                msg: "failed to stop the subsystem".to_string(),
            })
        } else {
            Ok(())
        }
    }

    #[instrument(debug)]
    pub fn destroy_all() {
        Reactors::master().send_future(async {
            NvmfSubsystem::first().iter().for_each(|s| s.destroy());
            NVMF_TGT.with(|t| {
                let mut tgt = t.borrow_mut();
                tgt.next_state()
            })
        });
    }

    pub fn first() -> Option<NvmfSubsystem> {
        NVMF_TGT.with(|t| {
            let ss = unsafe {
                spdk_nvmf_subsystem_get_first(t.borrow().tgt_as_ptr())
            };
            if ss.is_null() {
                None
            } else {
                Some(NvmfSubsystem(ss))
            }
        })
    }

    pub fn nqn_lookup(uuid: &str) -> Option<NvmfSubsystem> {
        let nqn = gen_nqn(uuid);
        NvmfSubsystem::first()
            .into_iter()
            .find(|s| s.get_nqn() == nqn)
    }

    pub fn bdev(&self) -> Bdev {
        let ns = unsafe { spdk_nvmf_subsystem_get_first_ns(self.0) };
        let b = unsafe { spdk_nvmf_ns_get_bdev(ns) };
        if b.is_null() {
            panic!("no bdev");
        }
        Bdev::from(b)
    }

    fn listeners_to_vec(&self) -> Option<Vec<TransportID>> {
        // fix this mess
        unsafe {
            let mut listener = spdk_nvmf_subsystem_get_first_listener(self.0);
            if listener.is_null() {
                None
            } else {
                let mut ids = vec![TransportID(
                    *spdk_nvmf_subsystem_listener_get_trid(listener),
                )];

                loop {
                    listener =
                        spdk_nvmf_subsystem_get_next_listener(self.0, listener);
                    if !listener.is_null() {
                        ids.push(TransportID(
                            *spdk_nvmf_subsystem_listener_get_trid(listener),
                        ));
                        continue;
                    } else {
                        break;
                    }
                }
                Some(ids)
            }
        }
    }

    pub fn uri_endpoints(&self) -> Option<Vec<String>> {
        if let Some(v) = self.listeners_to_vec() {
            let nqn = self.get_nqn();
            Some(
                v.iter()
                    .map(|t| format!("{}/{}", t, nqn))
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        }
    }
}

fn gen_nqn(id: &str) -> String {
    format!("nqn.2019-05.io.openebs:{}", id)
}
