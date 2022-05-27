#[macro_use]
extern crate ioctl_gen;
#[macro_use]
extern crate tracing;
extern crate nix;
#[macro_use]
extern crate serde;
extern crate function_name;
extern crate serde_json;
extern crate snafu;
extern crate spdk_rs;

#[macro_use]
pub mod core;
pub mod bdev;
pub mod delay;
pub use spdk_rs::ffihelper;
pub mod constants;
pub mod grpc;
pub mod host;
pub mod jsonrpc;
pub mod logger;
pub mod lvs;
pub mod nexus_uri;
pub mod persistent_store;
pub mod pool;
pub mod rebuild;
pub mod replica;
mod sleep;
pub mod store;
pub mod subsys;
pub mod target;

/// TODO
#[macro_export]
macro_rules! CPS_INIT {
    () => {
        #[link_section = ".init_array"]
        #[used]
        pub static INITIALIZE: extern "C" fn() = ::io_engine::cps_init;
    };
}

pub extern "C" fn cps_init() {
    subsys::register_subsystem();
    bdev::nexus::register_module();
    bdev::null_ng::register();
}
