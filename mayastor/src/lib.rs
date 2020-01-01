#[macro_use]
extern crate ioctl_gen;
#[macro_use]
extern crate lazy_static;
extern crate nix;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate snafu;
extern crate spdk_sys;

use std::{os::raw::c_void, time::Duration};

pub mod aio_dev;
pub mod app;
pub mod bdev;
pub mod descriptor;
pub mod dma;
pub mod environment;
pub mod event;
pub mod executor;
pub mod iscsi_dev;
pub mod iscsi_target;
pub mod jsonrpc;
pub mod logger;
pub mod nexus_uri;
pub mod nvmf_dev;
pub mod nvmf_target;
pub mod poller;
pub mod pool;
pub mod rebuild;
pub mod replica;
pub mod spdklog;

#[macro_export]
macro_rules! CPS_INIT {
    () => {
        #[link_section = ".init_array"]
        #[used]
        pub static INITIALIZE: extern "C" fn() = ::mayastor::cps_init;
    };
}

pub extern "C" fn cps_init() {
    bdev::nexus::register_module();
}

/// Delay function called from the spdk poller to prevent draining of cpu
/// in cases when performance is not a priority (i.e. unit tests).
extern "C" fn developer_delay(_ctx: *mut c_void) -> i32 {
    std::thread::sleep(Duration::from_millis(1));
    0
}
