use std::ffi::c_void;

use rand::seq::SliceRandom;

use crate::{
    core::{Cores, Mthread, Reactors},
    ffihelper::AsStr,
    subsys::{
        nvmf::{
            poll_groups::PollGroup,
            subsystem::NvmfSubsystem,
            transport,
            transport::{get_ipv4_address, TransportID},
            Error,
            NVMF_PGS,
            NVMF_TGT,
        },
        Config,
    },
};
use spdk_sys::{
    spdk_env_get_core_count,
    spdk_nvmf_get_optimal_poll_group,
    spdk_nvmf_poll_group,
    spdk_nvmf_poll_group_add,
    spdk_nvmf_poll_group_destroy,
    spdk_nvmf_qpair,
    spdk_nvmf_qpair_disconnect,
    spdk_nvmf_target_opts,
    spdk_nvmf_tgt,
    spdk_nvmf_tgt_accept,
    spdk_nvmf_tgt_create,
    spdk_nvmf_tgt_destroy,
    spdk_nvmf_tgt_listen,
    spdk_nvmf_tgt_stop_listen,
    spdk_poller,
    spdk_poller_register_named,
    spdk_poller_unregister,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};
use tracing::instrument;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Target {
    tgt: *mut spdk_nvmf_tgt,
    accepter_poller: *mut spdk_poller,
    poll_group_count: u16,
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
    Init,
    PollGroupInit,
    AcceptorInit,
    AddTransport,
    AddListener,
    Running,
    ShutdownSubsystems,
    DestroyPgs,
    Shutdown,
    ShutdownCompleted,
    Invalid,
}

impl Target {
    pub fn new() -> Self {
        assert_eq!(Cores::current(), Cores::first());
        Self {
            tgt: std::ptr::null_mut(),
            accepter_poller: std::ptr::null_mut(),
            poll_group_count: 0,
            next_state: TargetState::Init,
        }
    }

    pub fn init(&mut self) -> Result<()> {
        let cfg = Config::by_ref();
        let tgt_ptr: Box<spdk_nvmf_target_opts> =
            cfg.nvmf_tcp_tgt_conf.clone().into();

        self.tgt =
            unsafe { spdk_nvmf_tgt_create(&*tgt_ptr as *const _ as *mut _) };
        if self.tgt.is_null() {
            return Err(Error::CreateTarget {
                msg: "tgt pointer is None".to_string(),
            });
        }
        self.next_state();
        Ok(())
    }
    #[instrument(level = "debug")]
    pub fn next_state(&mut self) {
        match self.next_state {
            TargetState::Init => {
                self.next_state = TargetState::PollGroupInit;
                self.init().unwrap();
            }
            TargetState::PollGroupInit => {
                self.next_state = TargetState::AcceptorInit;
                self.init_poll_groups();
            }
            TargetState::AcceptorInit => {
                self.next_state = TargetState::AddTransport;
                self.init_acceptor();
            }
            TargetState::AddTransport => {
                self.next_state = TargetState::AddListener;
                Reactors::master().send_future(async {
                    let result = transport::add_tcp_transport().await;
                    NVMF_TGT.with(|t| {
                        if result.is_err() {
                            t.borrow_mut().next_state = TargetState::Invalid;
                        }
                        t.borrow_mut().next_state();
                    });
                });
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

    pub fn init_acceptor(&mut self) {
        self.accepter_poller = unsafe {
            spdk_poller_register_named(
                Some(Self::acceptor_poll),
                self.tgt as *mut _,
                10000,
                "mayastor_nvmf_tgt_poller\0" as *const _ as *mut _,
            )
        };

        self.next_state();
    }
    pub fn init_poll_groups(&self) {
        Reactors::iter().for_each(|r| {
            if let Some(t) = Mthread::new(
                format!("mayastor_nvmf_tcp_pg_core_{}", r.core()),
                r.core(),
            ) {
                r.send_future(Self::create_poll_group(self.tgt, t));
            }
        });
    }

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

    fn select_pg(qp: *mut spdk_nvmf_qpair) -> Result<PollGroup> {
        NVMF_PGS.with(|pgs| {
            let pgs = pgs.borrow();
            if let Some(pg) = pgs.iter().find(|pg| {
                let gr = unsafe { spdk_nvmf_get_optimal_poll_group(qp) };
                pg.group_ptr() == gr
            }) {
                Ok(pg.clone())
            } else {
                let pg = pgs.choose(&mut rand::thread_rng()).unwrap().clone();
                Ok(pg)
            }
        })
    }

    // this will be removed in future releases as this is going to be pushed
    // down into libnvmf
    extern "C" fn new_qpair(qp: *mut spdk_nvmf_qpair, _: *mut c_void) {
        extern "C" fn qp_add_to_pg(pg: *mut c_void) {
            let ctx: Box<(*mut spdk_nvmf_poll_group, *mut spdk_nvmf_qpair)> =
                unsafe { Box::from_raw(pg as *mut _) };

            unsafe {
                if spdk_nvmf_poll_group_add(ctx.0, ctx.1) != 0 {
                    spdk_nvmf_qpair_disconnect(
                        ctx.1,
                        None,
                        std::ptr::null_mut(),
                    );
                }
            }
        }

        NVMF_TGT.with(|tgt| {
            if tgt.borrow().next_state != TargetState::Running {
                warn!("TGT is not running, dropping connection request");
                unsafe {
                    spdk_nvmf_qpair_disconnect(qp, None, std::ptr::null_mut())
                };
                return;
            }
        });

        Self::select_pg(qp)
            .map(|p| {
                let ctx = Box::new((p.group_ptr(), qp));
                p.thread
                    .send_msg(qp_add_to_pg, Box::into_raw(ctx) as *mut c_void)
            })
            .unwrap();
    }

    extern "C" fn acceptor_poll(tgt: *mut c_void) -> i32 {
        let tgt = tgt as *mut spdk_nvmf_tgt;
        unsafe {
            spdk_nvmf_tgt_accept(
                tgt,
                Some(Self::new_qpair),
                std::ptr::null_mut(),
            )
        };

        0
    }

    /// Listen for incoming connections
    pub fn listen(&mut self) -> Result<()> {
        let cfg = Config::by_ref();
        let trid_nexus = TransportID::new(cfg.nexus_opts.nvmf_nexus_port);
        let rc = unsafe { spdk_nvmf_tgt_listen(self.tgt, trid_nexus.as_ptr()) };

        if rc != 0 {
            return Err(Error::CreateTarget {
                msg: "failed to back target".into(),
            });
        }

        let trid_replica = TransportID::new(cfg.nexus_opts.nvmf_replica_port);
        let rc =
            unsafe { spdk_nvmf_tgt_listen(self.tgt, trid_replica.as_ptr()) };

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

    pub fn stop_subsystems(&self) {
        NvmfSubsystem::destroy_all();
    }

    pub fn destroy_pgs(&mut self) {
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

    pub fn running(&mut self) {
        info!(
            "nvmf target accepting new connections and is ready to role..{}",
            '\u{1F483}'
        );
        unsafe { spdk_subsystem_init_next(0) }
    }

    pub fn shutdown(&mut self) {
        extern "C" fn destroy_cb(_arg: *mut c_void, _status: i32) {
            info!("NVMe-oF target shutdown completed");
            unsafe {
                spdk_subsystem_fini_next();
            }
        }

        unsafe { spdk_poller_unregister(&mut self.accepter_poller) };

        let cfg = Config::by_ref();
        let trid_nexus = TransportID::new(cfg.nexus_opts.nvmf_nexus_port);
        let trid_replica = TransportID::new(cfg.nexus_opts.nvmf_replica_port);

        unsafe { spdk_nvmf_tgt_stop_listen(self.tgt, trid_replica.as_ptr()) };
        unsafe { spdk_nvmf_tgt_stop_listen(self.tgt, trid_nexus.as_ptr()) };

        unsafe {
            spdk_nvmf_tgt_destroy(
                self.tgt,
                Some(destroy_cb),
                std::ptr::null_mut(),
            )
        }
    }
    #[instrument(level = "debug")]
    pub fn start_shutdown(&mut self) {
        self.next_state = TargetState::ShutdownSubsystems;
        Reactors::master().send_future(async {
            NVMF_TGT.with(|tgt| {
                tgt.borrow_mut().next_state();
            });
        });
    }

    pub fn tgt_as_ptr(&self) -> *mut spdk_nvmf_tgt {
        self.tgt
    }
}
