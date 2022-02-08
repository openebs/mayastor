use std::{
    cell::RefCell,
    ffi::{c_void, CString},
    ptr::NonNull,
};

use nix::errno::Errno;

use spdk_rs::libspdk::{
    spdk_env_get_core_count,
    spdk_nvmf_listen_opts,
    spdk_nvmf_listen_opts_init,
    spdk_nvmf_poll_group_destroy,
    spdk_nvmf_subsystem_create,
    spdk_nvmf_subsystem_set_mn,
    spdk_nvmf_target_opts,
    spdk_nvmf_tgt,
    spdk_nvmf_tgt_create,
    spdk_nvmf_tgt_destroy,
    spdk_nvmf_tgt_listen_ext,
    spdk_nvmf_tgt_stop_listen,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
    SPDK_NVMF_DISCOVERY_NQN,
    SPDK_NVMF_SUBTYPE_DISCOVERY,
};

use crate::{
    core::{Cores, Mthread, Reactor, Reactors},
    ffihelper::{AsStr, FfiResult},
    subsys::{
        nvmf::{
            poll_groups::PollGroup,
            subsystem::NvmfSubsystem,
            transport,
            transport::{get_ipv4_address, TransportId},
            Error,
            NVMF_PGS,
        },
        Config,
    },
};

type Result<T, E = Error> = std::result::Result<T, E>;

thread_local! {
pub (crate) static NVMF_TGT: RefCell<Target> = RefCell::new(Target::new());
}

#[derive(Debug)]
pub struct Target {
    /// the raw pointer to  our target
    pub(crate) tgt: NonNull<spdk_nvmf_tgt>,
    /// the number of poll groups created for this target
    poll_group_count: u16,
    /// The current state of the target
    next_state: TargetState,
}

impl Default for Target {
    fn default() -> Self {
        Target::new()
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TargetState {
    /// the initial state of the target, allocate main data structures
    Init,
    /// initialize the poll groups
    PollGroupInit,
    /// add transport configurations
    AddTransport,
    /// add the listener for the target
    AddListener,
    /// the running state where we are ready to serve new subsystems
    Running,
    ShutdownSubsystems,
    /// shutdown sequence has been started
    DestroyPgs,
    /// destroy portal groups
    Shutdown,
    /// shutdown has been completed
    ShutdownCompleted,
    /// an internal error has moved the target into the invalid state
    Invalid,
}

impl Target {
    /// we create a target by going through different stages. Its loosely
    /// modeled as the native target so that we can "creep in" changes as
    /// the API is not stable.
    ///
    /// Note a target cannot be constructed other than through the thread_local
    /// which is only run on the master core
    fn new() -> Self {
        assert_eq!(Cores::current(), Cores::first());
        Self {
            tgt: NonNull::dangling(),
            poll_group_count: 0,
            next_state: TargetState::Init,
        }
    }

    /// initialize the target and advance states
    fn init(&mut self) -> Result<()> {
        let cfg = Config::get();
        let tgt_ptr: Box<spdk_nvmf_target_opts> =
            cfg.nvmf_tcp_tgt_conf.clone().into();

        let tgt =
            unsafe { spdk_nvmf_tgt_create(&*tgt_ptr as *const _ as *mut _) };
        if tgt.is_null() {
            return Err(Error::CreateTarget {
                msg: "tgt pointer is None".to_string(),
            });
        }
        self.tgt = NonNull::new(tgt).unwrap();

        self.next_state();
        Ok(())
    }

    /// internally drive the target towards the next state
    pub(crate) fn next_state(&mut self) {
        match self.next_state {
            TargetState::Init => {
                self.next_state = TargetState::PollGroupInit;
                self.init().unwrap();
            }
            TargetState::PollGroupInit => {
                self.next_state = TargetState::AddTransport;
                self.init_poll_groups();
            }
            TargetState::AddTransport => {
                self.next_state = TargetState::AddListener;
                self.add_transport();
            }
            TargetState::AddListener => {
                self.next_state = TargetState::Running;
                self.listen()
                    .map_err(|e| {
                        error!("failed to listen on address {}", e);
                        self.next_state = TargetState::Invalid;
                    })
                    .unwrap();
            }
            TargetState::Running => {
                self.running();
            }
            TargetState::ShutdownSubsystems => {
                self.next_state = TargetState::DestroyPgs;
                self.stop_subsystems();
            }
            TargetState::DestroyPgs => {
                self.next_state = TargetState::Shutdown;
                self.destroy_pgs();
            }
            TargetState::Shutdown => {
                self.next_state = TargetState::ShutdownCompleted;
                self.shutdown();
            }
            TargetState::Invalid => {
                info!("Target configuration failed... doing nothing");
                unsafe { spdk_subsystem_init_next(1) }
            }

            _ => panic!("Invalid target state"),
        };
    }

    /// add the transport to the target
    fn add_transport(&self) {
        Reactors::master().send_future(async {
            let result = transport::add_tcp_transport().await;
            NVMF_TGT.with(|t| {
                if result.is_err() {
                    t.borrow_mut().next_state = TargetState::Invalid;
                }
                t.borrow_mut().next_state();
            });
        })
    }

    /// init the poll groups per core
    fn init_poll_groups(&self) {
        Reactors::iter().for_each(|r| {
            if let Some(t) = Mthread::new(
                format!("mayastor_nvmf_tcp_pg_core_{}", r.core()),
                r.core(),
            ) {
                r.send_future(Self::create_poll_group(self.tgt.as_ptr(), t));
            }
        });
    }

    /// init the poll groups implementation
    async fn create_poll_group(tgt: *mut spdk_nvmf_tgt, mt: Mthread) {
        mt.with(|| {
            let pg = PollGroup::new(tgt, mt);

            Reactors::master().send_future(async move {
                NVMF_TGT.with(|tgt| {
                    let mut tgt = tgt.borrow_mut();
                    NVMF_PGS.with(|p| p.borrow_mut().push(pg));
                    tgt.poll_group_count += 1;
                    if tgt.poll_group_count
                        == unsafe { spdk_env_get_core_count() as u16 }
                    {
                        Reactors::master().send_future(async {
                            NVMF_TGT.with(|tgt| {
                                tgt.borrow_mut().next_state();
                            })
                        })
                    }
                });
            });
        });
    }

    /// Listen for incoming connections by default we only listen on the replica
    /// port
    fn listen(&mut self) -> Result<()> {
        let cfg = Config::get();
        let trid_nexus = TransportId::new(cfg.nexus_opts.nvmf_nexus_port);
        let mut opts = spdk_nvmf_listen_opts::default();
        unsafe {
            spdk_nvmf_listen_opts_init(
                &mut opts,
                std::mem::size_of::<spdk_nvmf_listen_opts>() as u64,
            );
        }
        let rc = unsafe {
            spdk_nvmf_tgt_listen_ext(
                self.tgt.as_ptr(),
                trid_nexus.as_ptr(),
                &mut opts,
            )
        };

        if rc != 0 {
            return Err(Error::CreateTarget {
                msg: "failed to back target".into(),
            });
        }

        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);
        let rc = unsafe {
            spdk_nvmf_tgt_listen_ext(
                self.tgt.as_ptr(),
                trid_replica.as_ptr(),
                &mut opts,
            )
        };

        if rc != 0 {
            return Err(Error::CreateTarget {
                msg: "failed to front target".into(),
            });
        }
        info!(
            "nvmf target listening on {}:({},{})",
            get_ipv4_address().unwrap(),
            trid_nexus.trsvcid.as_str(),
            trid_replica.trsvcid.as_str(),
        );
        self.next_state();
        Ok(())
    }

    /// enable discovery for the target -- note that the discovery system is not
    /// started
    fn enable_discovery(&self) {
        debug!("enabling discovery for target");
        let discovery = unsafe {
            NvmfSubsystem::from(spdk_nvmf_subsystem_create(
                self.tgt.as_ptr(),
                SPDK_NVMF_DISCOVERY_NQN.as_ptr() as *const std::os::raw::c_char,
                SPDK_NVMF_SUBTYPE_DISCOVERY,
                0,
            ))
        };

        let mn = CString::new("Mayastor NVMe controller").unwrap();
        unsafe {
            spdk_nvmf_subsystem_set_mn(discovery.0.as_ptr(), mn.as_ptr())
        }
        .to_result(|e| Error::Subsystem {
            source: Errno::from_i32(e),
            nqn: "discovery".into(),
            msg: "failed to set serial".into(),
        })
        .unwrap();

        discovery.allow_any(true);

        Reactor::block_on(async {
            let _ = discovery.start().await.unwrap();
        });
    }

    /// stop all subsystems on this target we are borrowed here
    fn stop_subsystems(&self) {
        let tgt = self.tgt.as_ptr();
        Reactors::master().send_future(async move {
            NvmfSubsystem::stop_all(tgt).await;
            debug!("all subsystems stopped!");
            NvmfSubsystem::destroy_all();
        });
    }

    /// destroy all portal groups on this target
    fn destroy_pgs(&mut self) {
        extern "C" fn pg_destroy_done(_arg: *mut c_void, _arg1: i32) {
            Reactors::master().send_future(async {
                NVMF_TGT.with(|t| {
                    let mut tgt = t.borrow_mut();
                    tgt.poll_group_count -= 1;
                    if tgt.poll_group_count == 0 {
                        debug!("all pgs destroyed {:?}", tgt);
                        tgt.next_state()
                    }
                })
            })
        }

        extern "C" fn pg_destroy(arg: *mut c_void) {
            unsafe {
                let pg = Box::from_raw(arg as *mut PollGroup);
                spdk_nvmf_poll_group_destroy(
                    pg.group_ptr(),
                    Some(pg_destroy_done),
                    std::ptr::null_mut(),
                )
            }
        }

        NVMF_PGS.with(|t| {
            t.borrow().iter().for_each(|pg| {
                trace!("destroying pg: {:?}", pg);
                pg.thread.send_msg(
                    pg_destroy,
                    Box::into_raw(Box::new(pg.clone())) as *mut _,
                );
            });
        })
    }

    /// final state for the target during init
    pub fn running(&mut self) {
        self.enable_discovery();
        info!(
            "nvmf target accepting new connections and is ready to roll..{}",
            '\u{1F483}'
        );

        unsafe { spdk_subsystem_init_next(0) }
    }

    ///  shutdown procedure
    fn shutdown(&mut self) {
        extern "C" fn destroy_cb(_arg: *mut c_void, _status: i32) {
            info!("NVMe-oF target shutdown completed");
            unsafe {
                spdk_subsystem_fini_next();
            }
        }

        let cfg = Config::get();
        let trid_nexus = TransportId::new(cfg.nexus_opts.nvmf_nexus_port);
        let trid_replica = TransportId::new(cfg.nexus_opts.nvmf_replica_port);

        unsafe {
            spdk_nvmf_tgt_stop_listen(self.tgt.as_ptr(), trid_replica.as_ptr())
        };

        unsafe {
            spdk_nvmf_tgt_stop_listen(self.tgt.as_ptr(), trid_nexus.as_ptr())
        };

        unsafe {
            spdk_nvmf_tgt_destroy(
                self.tgt.as_ptr(),
                Some(destroy_cb),
                std::ptr::null_mut(),
            )
        }
    }

    /// start the shutdown of the target and subsystems
    pub(crate) fn start_shutdown(&mut self) {
        self.next_state = TargetState::ShutdownSubsystems;
        Reactors::master().send_future(async {
            NVMF_TGT.with(|tgt| {
                tgt.borrow_mut().next_state();
            });
        });
    }
}
