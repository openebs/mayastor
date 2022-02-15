//!
//! Main file to register additional subsystems

pub use config::{
    opts::{NexusOpts, NvmeBdevOpts},
    pool::PoolConfig,
    Config,
    ConfigSubsystem,
};
pub use nvmf::{
    create_snapshot,
    set_snapshot_time,
    Error as NvmfError,
    NvmeCpl,
    NvmfReq,
    NvmfSubsystem,
    SubType,
    Target as NvmfTarget,
};
use spdk_rs::libspdk::{
    spdk_add_subsystem,
    spdk_add_subsystem_depend,
    spdk_subsystem_depend,
};

pub use registration::{
    registration_grpc::Registration,
    RegistrationSubsystem,
};

use crate::subsys::nvmf::Nvmf;

mod config;
mod nvmf;
mod registration;

/// Register initial subsystems
pub(crate) fn register_subsystem() {
    unsafe { spdk_add_subsystem(ConfigSubsystem::new().0) }
    unsafe {
        let mut depend = Box::new(spdk_subsystem_depend::default());
        depend.name = b"mayastor_nvmf_tgt\0" as *const u8 as *mut _;
        depend.depends_on = b"bdev\0" as *const u8 as *mut _;
        spdk_add_subsystem(Nvmf::new().0);
        spdk_add_subsystem_depend(Box::into_raw(depend));
    }
    RegistrationSubsystem::register();
}
