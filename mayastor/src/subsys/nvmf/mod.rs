//!
//!  The target can make use of several transports. Using different transports
//! allows us to  switch between, say, TCP and RDMA.
//!  To have the target listen, we specify a transport_id which references the
//! transport
use std::cell::RefCell;

use nix::errno::Errno;
use snafu::Snafu;

use poll_groups::PollGroup;
use spdk_sys::{
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};
pub use subsystem::NvmfSubsystem;
pub use target::Target;

use crate::{core::Bdev, subsys::Config};

mod poll_groups;
mod subsystem;
mod target;
mod transport;

pub struct Nvmf(pub(crate) *mut spdk_subsystem);

impl Default for Nvmf {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create nvmf target {}", msg))]
    CreateTarget { msg: String },
    #[snafu(display(
        "Failed to destroy nvmf target {}: {}",
        endpoint,
        source
    ))]
    DestroyTarget { source: Errno, endpoint: String },
    #[snafu(display("Failed to create poll groups {}", msg))]
    PgError { msg: String },
    #[snafu(display("Failded to create transport {}", msg))]
    Transport { source: Errno, msg: String },
    #[snafu(display("Failed to create subsystem for {} {}", nqn, msg))]
    Subsystem { nqn: String, msg: String },
    #[snafu(display("Failed to create share for  {} {}", bdev, msg))]
    Share { bdev: Bdev, msg: String },
    #[snafu(display("Failed to add namespace for  {} {}", bdev, msg))]
    Namespace { bdev: Bdev, msg: String },
}

thread_local! {
    pub (crate) static NVMF_TGT: RefCell<Target> = RefCell::new(Target::new());
    pub (crate) static NVMF_PGS: RefCell<Vec<PollGroup>> = RefCell::new(Vec::new());
}

impl Nvmf {
    extern "C" fn init() {
        debug!("mayastor nvmf subsystem init");

        if Config::by_ref().nexus_opts.nvmf_enable {
            NVMF_TGT.with(|tgt| {
                tgt.borrow_mut().next_state();
            });
        } else {
            unsafe { spdk_subsystem_init_next(0) }
        }
    }

    extern "C" fn fini() {
        debug!("mayastor nvmf fini");
        if Config::by_ref().nexus_opts.nvmf_enable {
            NVMF_TGT.with(|tgt| {
                tgt.borrow_mut().start_shutdown();
            });
        } else {
            unsafe { spdk_subsystem_fini_next() }
        }
    }

    pub fn new() -> Self {
        debug!("creating Mayastor nvmf subsystem...");
        let mut ss = Box::new(spdk_subsystem::default());
        ss.name = b"mayastor_nvmf_tgt\x00" as *const u8 as *const libc::c_char;
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = None;
        Self(Box::into_raw(ss))
    }
}
