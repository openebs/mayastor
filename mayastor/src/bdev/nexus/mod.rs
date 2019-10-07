#![allow(clippy::vec_box)]
use crate::{
    bdev::nexus::{
        nexus_bdev::Nexus,
        nexus_fn_table::NexusFnTable,
        nexus_module::NexusModule,
        nexus_rpc::register_rpc_methods,
    },
    nexus_uri::{self, UriError},
};
use std::option::NoneError;
#[derive(Debug)]
pub enum Error {
    /// Nobody knows
    Internal(String),
    /// spdk functions are called on a non SPDK thread
    InvalidThread,
    /// OOM but its not possible to know if this is spdk_dma_malloc() or
    /// malloc()
    OutOfMemory,
    /// the bdev is already claimed by some other parent
    AlreadyClaimed,
    /// the bdev can can only be opened RO as its been claimed with write
    /// options already
    ReadOnly,
    /// bdev does not exist
    NotFound,
    /// Invalid arguments with the bdev struct
    Invalid,
    /// the bdev creation failed
    CreateFailed,
    /// a bdev with either the same name or alias already exists
    Exists,
    /// the child bdev for the nexus already exits
    ChildExists,
    /// the nexus is does not have enough children to come online
    NexusIncomplete,
    /// error during serial or deserialize
    SerDerError,
}

impl From<std::ffi::NulError> for Error {
    fn from(_: std::ffi::NulError) -> Self {
        Error::OutOfMemory
    }
}

impl From<nexus_uri::UriError> for Error {
    fn from(_: UriError) -> Self {
        Error::Invalid
    }
}

impl From<bincode::Error> for Error {
    fn from(_e: bincode::Error) -> Self {
        Error::SerDerError
    }
}

/// Generic conversions of errors
/// SPDK uses different ENOXXX at various levels the conversions here
/// are known to be consistent
impl From<i32> for Error {
    fn from(e: i32) -> Self {
        match e {
            libc::ENOMEM => Error::OutOfMemory,
            _ => Error::Internal(format!("errno {}", e)),
        }
    }
}

impl From<NoneError> for Error {
    fn from(_e: NoneError) -> Self {
        Error::Invalid
    }
}

pub mod nexus_bdev;
pub mod nexus_bdev_children;
mod nexus_channel;
mod nexus_child;
mod nexus_config;
mod nexus_fn_table;
mod nexus_io;
pub mod nexus_label;
pub mod nexus_module;
pub mod nexus_nbd;
pub mod nexus_rpc;

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
pub fn nexus_instance_new(
    name: String,
    size: u64,
    blksize: u32,
    children: Vec<String>,
) {
    let list = instances();
    if let Ok(nexus) = Nexus::new(&name, blksize, size, None, Some(&children)) {
        list.push(nexus);
    }
}
