#![allow(clippy::vec_box)]
use crate::bdev::nexus::{
    nexus_bdev::Nexus,
    nexus_fn_table::NexusFnTable,
    nexus_module::NexusModule,
    nexus_rpc::register_rpc_methods,
};

pub mod nexus_bdev;
pub mod nexus_bdev_children;
mod nexus_channel;
mod nexus_child;
mod nexus_config;
mod nexus_fn_table;
pub mod nexus_io;
pub mod nexus_label;
pub mod nexus_module;
pub mod nexus_nbd;
pub mod nexus_rebuild;
pub mod nexus_rpc;
pub mod nexus_share;

/// public function which simply calls register module
pub fn register_module() {
    register_rpc_methods();
    nexus_module::register_module()
}

/// get a reference to the module
pub fn module() -> Option<NexusModule> {
    nexus_module::NexusModule::current()
}

/// get a static ref to the fn table of the nexus module
pub fn fn_table() -> Option<&'static spdk_sys::spdk_bdev_fn_table> {
    Some(NexusFnTable::table())
}

/// get a reference to the global nexuses
pub fn instances() -> &'static mut Vec<Box<Nexus>> {
    nexus_module::NexusModule::get_instances()
}

/// function used to create a new nexus when parsing a config file
pub fn nexus_instance_new(name: String, size: u64, children: Vec<String>) {
    let list = instances();
    list.push(Nexus::new(&name, size, None, Some(&children)));
}
