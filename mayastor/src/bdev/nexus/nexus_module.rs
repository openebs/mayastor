use std::{cell::UnsafeCell, ffi::CString};

use once_cell::sync::{Lazy, OnceCell};

use spdk_sys::{
    spdk_bdev_module,
    spdk_bdev_module_examine_done,
    spdk_bdev_module_list_add,
    spdk_get_thread,
};

use crate::{
    bdev::nexus::{
        nexus_bdev::{Nexus, NexusState},
        nexus_io::NioCtx,
    },
    core::Bdev,
};

const NEXUS_NAME: &str = "NEXUS_CAS_MODULE";

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
        module.examine_config = Some(Self::examine);
        module.examine_disk = None;
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

    extern "C" fn examine(new_device: *mut spdk_sys::spdk_bdev) {
        let name = Bdev::from(new_device).name();
        let instances = Self::get_instances();

        // dont examine ourselves

        if instances.iter().any(|n| n.name == name) {
            unsafe {
                spdk_bdev_module_examine_done(
                    NEXUS_MODULE.0 as *const _ as *mut _,
                )
            }
            return;
        }

        instances
            .iter()
            .filter(|n| n.state == NexusState::Init)
            .any(|bdev| {
                let n = unsafe { Nexus::from_raw((*bdev.bdev.as_ptr()).ctxt) };
                if n.examine_child(&name) {
                    let _r = n.open();
                    return true;
                }
                false
            });

        unsafe {
            spdk_bdev_module_examine_done(NEXUS_MODULE.0 as *const _ as *mut _)
        }
    }

    extern "C" fn nexus_ctx_size() -> i32 {
        std::mem::size_of::<NioCtx>() as i32
    }
}

pub fn register_module() {
    unsafe {
        spdk_bdev_module_list_add((NEXUS_MODULE.0) as *const _ as *mut _);
    }
}
