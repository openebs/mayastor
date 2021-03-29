use std::{cell::UnsafeCell, ffi::CString};

use once_cell::sync::{Lazy, OnceCell};
use serde_json::json;

use spdk_sys::{
    spdk_bdev_module,
    spdk_bdev_module_list_add,
    spdk_get_thread,
    spdk_json_write_ctx,
    spdk_json_write_val_raw,
};

use crate::bdev::nexus::{nexus_bdev::Nexus, nexus_io::NioCtx};

use super::instances;

pub const NEXUS_NAME: &str = "NEXUS_CAS_MODULE";

pub static NEXUS_MODULE: Lazy<NexusModule> = Lazy::new(NexusModule::new);

#[derive(Default, Debug)]
pub struct NexusInstances {
    inner: UnsafeCell<Vec<Box<Nexus>>>,
}

#[derive(Debug)]
pub struct NexusModule(*mut spdk_bdev_module);

unsafe impl Sync for NexusModule {}
unsafe impl Sync for NexusInstances {}

unsafe impl Send for NexusModule {}
unsafe impl Send for NexusInstances {}

impl Default for NexusModule {
    fn default() -> Self {
        Self::new()
    }
}

impl NexusModule {
    /// construct a new NexusModule instance and setup main properties
    /// as well as the function table and json-rpc methods if any
    pub fn new() -> Self {
        let mut module = Box::new(spdk_bdev_module::default());
        module.name = c_str!(NEXUS_NAME);

        module.async_init = false;
        module.async_fini = false;
        module.module_init = Some(Self::nexus_mod_init);
        module.module_fini = Some(Self::nexus_mod_fini);
        module.get_ctx_size = Some(Self::nexus_ctx_size);
        module.examine_config = None;
        module.examine_disk = None;
        module.config_json = Some(Self::config_json);
        NexusModule(Box::into_raw(module))
    }

    pub fn as_ptr(&self) -> *mut spdk_bdev_module {
        self.0
    }

    pub fn from_null_checked(b: *mut spdk_bdev_module) -> Option<Self> {
        if b.is_null() {
            None
        } else {
            Some(NexusModule(b))
        }
    }

    /// obtain a pointer to the raw bdev module
    pub fn current() -> Option<Self> {
        let c_name = std::ffi::CString::new(NEXUS_NAME).unwrap();
        let module =
            unsafe { spdk_sys::spdk_bdev_module_list_find(c_name.as_ptr()) };
        NexusModule::from_null_checked(module)
    }

    /// return instances, we ensure that this can only ever be called on a
    /// properly allocated thread
    pub fn get_instances() -> &'static mut Vec<Box<Nexus>> {
        let thread = unsafe { spdk_get_thread() };
        if thread.is_null() {
            panic!("not called from SPDK thread")
        }

        static NEXUS_INSTANCES: OnceCell<NexusInstances> = OnceCell::new();

        let global_instances = NEXUS_INSTANCES.get_or_init(|| NexusInstances {
            inner: UnsafeCell::new(Vec::new()),
        });

        unsafe { &mut *global_instances.inner.get() }
    }
}
/// Implements the bdev module call back functions to register the driver to
/// SPDK
impl NexusModule {
    extern "C" fn nexus_mod_init() -> i32 {
        info!("Initializing Nexus CAS Module");
        crate::bdev::nexus::nexus_config::parse_ini_config_file();
        0
    }

    extern "C" fn nexus_mod_fini() {
        info!("Unloading Nexus CAS Module");
        let _ = unsafe { CString::from_raw((*(NEXUS_MODULE.0)).name as _) };
        Self::get_instances().clear();
    }

    extern "C" fn nexus_ctx_size() -> i32 {
        std::mem::size_of::<NioCtx>() as i32
    }

    /// creates a JSON object that can be applied to mayastor that
    /// will construct the nexus object and its children. Note that
    /// the nexus implicitly tries to create the children as such
    /// you should not have any iSCSI create related calls that
    /// construct children in the config file.
    extern "C" fn config_json(w: *mut spdk_json_write_ctx) -> i32 {
        instances().iter().for_each(|nexus| {
            let uris = nexus
                .children
                .iter()
                .map(|c| c.get_name().to_string())
                .collect::<Vec<String>>();

            let json = json!({
                "method": "create_nexus",
                "params": {
                    "name" : nexus.name,
                    "uuid" : nexus.bdev.uuid_as_string(),
                    "children" : uris,
                    "size": nexus.size,
                },
            });

            let data =
                CString::new(serde_json::to_string(&json).unwrap()).unwrap();
            unsafe {
                spdk_json_write_val_raw(
                    w,
                    data.as_ptr() as *const _,
                    data.as_bytes().len() as u64,
                );
            }
        });
        0
    }
}

pub fn register_module() {
    unsafe {
        spdk_bdev_module_list_add((NEXUS_MODULE.0) as *const _ as *mut _);
    }
}
