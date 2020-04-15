#[macro_use]
extern crate ioctl_gen;
#[macro_use]
extern crate log;
extern crate nix;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate snafu;
extern crate spdk_sys;

pub mod bdev;
pub mod core;
pub mod delay;
pub mod ffihelper;
pub mod grpc;
pub mod jsonrpc;
pub mod logger;
pub mod nexus_uri;
pub mod pool;
/// Rebuild module
pub mod rebuild;
pub mod replica;
pub mod subsys;
pub mod target;
#[macro_export]
macro_rules! CPS_INIT {
    () => {
        #[link_section = ".init_array"]
        #[used]
        pub static INITIALIZE: extern "C" fn() = ::mayastor::cps_init;
    };
}

pub extern "C" fn cps_init() {
    subsys::register_subsystem();
    bdev::nexus::register_module();
}
