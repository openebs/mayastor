use std::{
    ffi::{c_void, CString},
    fmt::{self, Debug, Display, Formatter},
    mem::size_of,
    ptr::{self, NonNull},
};

use futures::channel::oneshot;
use nix::errno::Errno;

use spdk_rs::libspdk::{
    nvmf_subsystem_find_listener,
    nvmf_subsystem_set_ana_state,
    nvmf_subsystem_set_cntlid_range,
    spdk_bdev_nvme_opts,
    spdk_nvmf_ns_get_bdev,
    spdk_nvmf_ns_opts,
    spdk_nvmf_subsystem,
    spdk_nvmf_subsystem_add_listener,
    spdk_nvmf_subsystem_add_ns_ext,
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
    spdk_nvmf_subsystem_set_ana_reporting,
    spdk_nvmf_subsystem_set_mn,
    spdk_nvmf_subsystem_set_sn,
    spdk_nvmf_subsystem_start,
    spdk_nvmf_subsystem_stop,
    spdk_nvmf_tgt,
    SPDK_NVMF_SUBTYPE_DISCOVERY,
    SPDK_NVMF_SUBTYPE_NVME,
};

use crate::{
    constants::{NVME_CONTROLLER_MODEL_ID, NVME_NQN_PREFIX},
    core::{Bdev, Reactors, UntypedBdev},
    ffihelper::{cb_arg, AsStr, FfiResult, IntoCString},
    subsys::{
        nvmf::{transport::TransportId, Error, NVMF_TGT},
        Config,
    },
};

#[derive(Debug, PartialOrd, PartialEq)]
pub enum SubType {
    Nvme,
    Discovery,
}

impl Display for SubType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            SubType::Nvme => write!(f, "NVMe"),
            SubType::Discovery => write!(f, "Discovery"),
        }
    }
}

pub struct NvmfSubsystem(pub(crate) NonNull<spdk_nvmf_subsystem>);
pub struct NvmfSubsystemIterator(*mut spdk_nvmf_subsystem);

impl Iterator for NvmfSubsystemIterator {
    type Item = NvmfSubsystem;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_null() {
            None
        } else {
            let current = self.0;
            self.0 = unsafe { spdk_nvmf_subsystem_get_next(current) };
            NonNull::new(current).map(NvmfSubsystem)
        }
    }
}

impl IntoIterator for NvmfSubsystem {
    type Item = NvmfSubsystem;
    type IntoIter = NvmfSubsystemIterator;

    fn into_iter(self) -> Self::IntoIter {
        NVMF_TGT.with(|t| {
            NvmfSubsystemIterator(unsafe {
                spdk_nvmf_subsystem_get_first(t.borrow().tgt.as_ptr())
            })
        })
    }
}

impl Debug for NvmfSubsystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        unsafe {
            f.debug_struct("NvmfSubsystem")
                .field("id", &self.0.as_ref().id)
                .field("subtype", &self.subtype().to_string())
                .field("subnqn", &self.0.as_ref().subnqn.as_str().to_string())
                .field("sn", &self.0.as_ref().sn.as_str().to_string())
                .field("mn", &self.0.as_ref().mn.as_str().to_string())
                .field(
                    "allow_any_host",
                    &self.0.as_ref().flags.allow_any_host(),
                )
                .field("ana_reporting", &self.0.as_ref().flags.ana_reporting())
                .field("listeners", &self.listeners_to_vec())
                .finish()
        }
    }
}

impl From<*mut spdk_nvmf_subsystem> for NvmfSubsystem {
    fn from(s: *mut spdk_nvmf_subsystem) -> Self {
        NvmfSubsystem(NonNull::new(s).unwrap())
    }
}

impl NvmfSubsystem {
    /// TODO
    pub fn try_from<T>(bdev: &Bdev<T>) -> Result<Self, Error>
    where
        T: spdk_rs::BdevOps,
    {
        if bdev.is_claimed() {
            return Err(Error::CreateTarget {
                msg: "already shared".to_string(),
            });
        }
        let ss = NvmfSubsystem::new(bdev.name())?;
        ss.set_ana_reporting(true)?;
        ss.allow_any(true);
        if let Err(e) = ss.add_namespace(bdev) {
            ss.destroy();
            return Err(e);
        }
        Ok(ss)
    }
}

/// Makes a subsystem serial number from a subsystem UUID or name.
fn make_sn<T: AsRef<[u8]>>(uuid: T) -> CString {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(uuid);
    let s = hasher.finalize().to_vec();
    // SPDK requires serial number string to be no more than 20 chars.
    let s = format!("DCS{:.17}", hex::encode_upper(&s));
    CString::new(s).unwrap()
}

impl NvmfSubsystem {
    /// create a new subsystem where the NQN is based on the UUID
    pub fn new(uuid: &str) -> Result<Self, Error> {
        let nqn = gen_nqn(uuid).into_cstring();
        let ss = NVMF_TGT
            .with(|t| {
                let tgt = t.borrow().tgt.as_ptr();
                unsafe {
                    spdk_nvmf_subsystem_create(
                        tgt,
                        nqn.as_ptr(),
                        SPDK_NVMF_SUBTYPE_NVME,
                        1,
                    )
                }
            })
            .to_result(|_| Error::Subsystem {
                source: Errno::EEXIST,
                nqn: uuid.into(),
                msg: "ss ptr is null".into(),
            })?;

        // Use truncated SHA256 digest of Bdev UUID or name for subsystem
        // serial number.
        let sn = if let Some(nn) = Bdev::<()>::lookup_by_name(uuid) {
            make_sn(nn.uuid().as_bytes())
        } else {
            make_sn(&uuid)
        };

        unsafe { spdk_nvmf_subsystem_set_sn(ss.as_ptr(), sn.as_ptr()) }
            .to_result(|e| Error::Subsystem {
                source: Errno::from_i32(e),
                nqn: uuid.into(),
                msg: "failed to set serial".into(),
            })?;

        let mn = CString::new(NVME_CONTROLLER_MODEL_ID).unwrap();
        unsafe { spdk_nvmf_subsystem_set_mn(ss.as_ptr(), mn.as_ptr()) }
            .to_result(|e| Error::Subsystem {
                source: Errno::from_i32(e),
                nqn: uuid.into(),
                msg: "failed to set model number".into(),
            })?;

        Ok(NvmfSubsystem(ss))
    }

    /// unfortunately, we cannot always use the bdev UUID which is a shame and
    /// mostly due to testing.
    pub fn new_with_uuid(
        uuid: &str,
        bdev: &UntypedBdev,
    ) -> Result<Self, Error> {
        let ss = NvmfSubsystem::new(uuid)?;
        ss.set_ana_reporting(true)?;
        ss.allow_any(true);
        ss.add_namespace(bdev)?;
        Ok(ss)
    }

    /// add the given bdev to this namespace
    pub fn add_namespace<T>(&self, bdev: &Bdev<T>) -> Result<(), Error>
    where
        T: spdk_rs::BdevOps,
    {
        let opts = spdk_nvmf_ns_opts {
            nguid: *bdev.uuid().as_bytes(),
            ..Default::default()
        };
        let bdev_cname = CString::new(bdev.name()).unwrap();
        let ns_id = unsafe {
            spdk_nvmf_subsystem_add_ns_ext(
                self.0.as_ptr(),
                bdev_cname.as_ptr(),
                &opts as *const _,
                size_of::<spdk_bdev_nvme_opts>() as u64,
                ptr::null_mut(),
            )
        };

        // the first namespace should be 1 and we do not (currently) use
        // more than one namespace

        if ns_id < 1 {
            Err(Error::Namespace {
                bdev: bdev.name().to_string(),
                msg: "failed to add namespace ID".to_string(),
            })
        } else {
            debug!(?bdev, ?ns_id, "added as namespace");
            Ok(())
        }
    }

    /// destroy the subsystem
    pub fn destroy(&self) -> i32 {
        unsafe {
            if (*self.0.as_ptr()).destroying {
                warn!("Subsystem destruction already started");
                return -libc::EALREADY;
            }
            spdk_nvmf_subsystem_destroy(
                self.0.as_ptr(),
                None,
                std::ptr::null_mut(),
            )
        }
    }

    /// Get NVMe subsystem's NQN
    pub fn get_nqn(&self) -> String {
        unsafe {
            spdk_nvmf_subsystem_get_nqn(self.0.as_ptr())
                .as_str()
                .to_string()
        }
    }

    /// allow any host to connect to the subsystem
    pub fn allow_any(&self, enable: bool) {
        unsafe {
            spdk_nvmf_subsystem_set_allow_any_host(self.0.as_ptr(), enable)
        };
    }

    /// enable Asymmetric Namespace Access (ANA) reporting
    pub fn set_ana_reporting(&self, enable: bool) -> Result<(), Error> {
        match std::env::var("NEXUS_NVMF_ANA_ENABLE") {
            Ok(s) => {
                if s != "1" {
                    return Ok(());
                }
            }
            Err(_) => {
                return Ok(());
            }
        }
        unsafe {
            spdk_nvmf_subsystem_set_ana_reporting(self.0.as_ptr(), enable)
        }
        .to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: format!("failed to set ANA reporting, enable {}", enable),
        })?;
        Ok(())
    }

    /// set controller ID range
    pub fn set_cntlid_range(
        &self,
        cntlid_min: u16,
        cntlid_max: u16,
    ) -> Result<(), Error> {
        unsafe {
            nvmf_subsystem_set_cntlid_range(
                self.0.as_ptr(),
                cntlid_min,
                cntlid_max,
            )
        }
        .to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: format!(
                "failed to set controller ID range [{}, {}]",
                cntlid_min, cntlid_max
            ),
        })?;
        Ok(())
    }

    // we currently allow all listeners to the subsystem
    async fn add_listener(&self) -> Result<(), Error> {
        extern "C" fn listen_cb(arg: *mut c_void, status: i32) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
            s.send(status).unwrap();
        }

        let cfg = Config::get();

        // dont yet enable both ports, IOW just add one transportID now

        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);

        let (s, r) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_subsystem_add_listener(
                self.0.as_ptr(),
                trid_replica.as_ptr(),
                Some(listen_cb),
                cb_arg(s),
            );
        }

        r.await.expect("listener callback gone").to_result(|e| {
            Error::Transport {
                source: Errno::from_i32(e),
                msg: "Failed to add listener".to_string(),
            }
        })
    }

    /// start the subsystem previously created -- note that we destroy it on
    /// failure to ensure the state is not in limbo and to avoid leaking
    /// resources
    pub async fn start(self) -> Result<String, Error> {
        extern "C" fn start_cb(
            ss: *mut spdk_nvmf_subsystem,
            arg: *mut c_void,
            status: i32,
        ) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
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

        self.add_listener().await?;

        let (s, r) = oneshot::channel::<i32>();

        unsafe {
            spdk_nvmf_subsystem_start(
                self.0.as_ptr(),
                Some(start_cb),
                cb_arg(s),
            )
        }
        .to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: "out of memory".to_string(),
        })?;

        r.await.unwrap().to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: "failed to start the subsystem".to_string(),
        })?;

        debug!(?self, "shared");
        Ok(self.get_nqn())
    }

    /// stop the subsystem
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
        unsafe {
            spdk_nvmf_subsystem_stop(self.0.as_ptr(), Some(stop_cb), cb_arg(s))
        }
        .to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: "out of memory".to_string(),
        })?;

        r.await.unwrap().to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: "failed to stop the subsystem".to_string(),
        })?;

        debug!("stopped {}", self.get_nqn());
        Ok(())
    }

    /// transition the subsystem to paused state
    /// intended to be a temporary state while changes are made
    pub async fn pause(&self) -> Result<(), Error> {
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

        unsafe {
            spdk_nvmf_subsystem_pause(
                self.0.as_ptr(),
                1,
                Some(pause_cb),
                cb_arg(s),
            )
        }
        .to_result(|e| Error::Subsystem {
            source: Errno::from_i32(-e),
            nqn: self.get_nqn(),
            msg: format!("subsystem_pause returned: {}", e),
        })?;

        r.await.unwrap().to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: self.get_nqn(),
            msg: "failed to pause the subsystem".to_string(),
        })
    }

    /// transition the subsystem to active state
    pub async fn resume(&self) -> Result<(), Error> {
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
            spdk_nvmf_subsystem_resume(
                self.0.as_ptr(),
                Some(resume_cb),
                cb_arg(s),
            )
        };

        if rc != 0 {
            return Err(Error::Subsystem {
                source: Errno::from_i32(-rc),
                nqn: self.get_nqn(),
                msg: format!("subsystem_resume returned: {}", rc),
            });
        }

        rc = r.await.unwrap();
        if rc != 0 {
            Err(Error::Subsystem {
                source: Errno::UnknownErrno,
                nqn: self.get_nqn(),
                msg: "failed to resume the subsystem".to_string(),
            })
        } else {
            Ok(())
        }
    }

    /// get ANA state
    pub async fn get_ana_state(&self) -> Result<u32, Error> {
        let cfg = Config::get();
        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);
        let listener = unsafe {
            nvmf_subsystem_find_listener(self.0.as_ptr(), trid_replica.as_ptr())
        };
        if listener.is_null() {
            Err(Error::Listener {
                nqn: self.get_nqn(),
                trid: trid_replica.to_string(),
            })
        } else {
            Ok(unsafe { *(*listener).ana_state })
        }
    }

    /// set ANA state: optimized, non_optimized, inaccessible
    /// subsystem must be in paused or inactive state
    pub async fn set_ana_state(&self, ana_state: u32) -> Result<(), Error> {
        extern "C" fn set_ana_state_cb(arg: *mut c_void, status: i32) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
            s.send(status).unwrap();
        }
        let cfg = Config::get();
        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);

        let (s, r) = oneshot::channel::<i32>();

        unsafe {
            nvmf_subsystem_set_ana_state(
                self.0.as_ptr(),
                trid_replica.as_ptr(),
                ana_state,
                0,
                Some(set_ana_state_cb),
                cb_arg(s),
            );
        }

        r.await
            .expect("Cancellation is not supported")
            .to_result(|e| Error::Subsystem {
                source: Errno::from_i32(-e),
                nqn: self.get_nqn(),
                msg: "failed to set_ana_state of the subsystem".to_string(),
            })
    }

    /// destroy all subsystems associated with our target, subsystems must be in
    /// stopped state
    pub fn destroy_all() {
        Reactors::master().send_future(async {
            // NvmfSubsystem::first().iter().for_each(|s| s.destroy());
            NVMF_TGT.with(|t| {
                let mut tgt = t.borrow_mut();
                tgt.next_state()
            })
        });
    }

    /// stop all subsystems
    pub async fn stop_all(tgt: *mut spdk_nvmf_tgt) {
        let ss = unsafe {
            NvmfSubsystem(
                NonNull::new(spdk_nvmf_subsystem_get_first(tgt)).unwrap(),
            )
        };
        for s in ss.into_iter() {
            s.stop().await.unwrap();
        }
    }

    /// Get the first subsystem within the system
    pub fn first() -> Option<NvmfSubsystem> {
        NVMF_TGT.with(|t| {
            let ss = unsafe {
                spdk_nvmf_subsystem_get_first(t.borrow().tgt.as_ptr())
            };

            if ss.is_null() {
                None
            } else {
                Some(NvmfSubsystem(NonNull::new(ss).unwrap()))
            }
        })
    }

    /// lookup a subsystem by its UUID
    pub fn nqn_lookup(uuid: &str) -> Option<NvmfSubsystem> {
        let nqn = gen_nqn(uuid);
        NvmfSubsystem::first()
            .unwrap()
            .into_iter()
            .find(|s| s.get_nqn() == nqn)
    }

    /// get the bdev associated with this subsystem -- we implicitly assume the
    /// first namespace
    pub fn bdev(&self) -> Option<UntypedBdev> {
        let ns = unsafe { spdk_nvmf_subsystem_get_first_ns(self.0.as_ptr()) };

        if ns.is_null() {
            return None;
        }

        unsafe { Bdev::checked_from_ptr(spdk_nvmf_ns_get_bdev(ns)) }
    }

    fn listeners_to_vec(&self) -> Option<Vec<TransportId>> {
        unsafe {
            let mut listener =
                spdk_nvmf_subsystem_get_first_listener(self.0.as_ptr());

            if listener.is_null() {
                return None;
            }

            let mut ids = vec![TransportId(
                *spdk_nvmf_subsystem_listener_get_trid(listener),
            )];

            loop {
                listener = spdk_nvmf_subsystem_get_next_listener(
                    self.0.as_ptr(),
                    listener,
                );
                if !listener.is_null() {
                    ids.push(TransportId(
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

    pub fn subtype(&self) -> SubType {
        unsafe {
            match self.0.as_ref().subtype {
                SPDK_NVMF_SUBTYPE_DISCOVERY => SubType::Discovery,
                SPDK_NVMF_SUBTYPE_NVME => SubType::Nvme,
                _ => panic!("unknown NVMe subtype"),
            }
        }
    }

    /// return the URI's this subsystem is listening on
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
    format!("{}:{}", NVME_NQN_PREFIX, id)
}
