#[macro_use]
extern crate ioctl_gen;
extern crate nix;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate snafu;
extern crate spdk_sys;

pub mod aio_dev;
pub mod app;
pub mod bdev;
pub mod delay;
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
