#![allow(clippy::vec_box)]
use lazy_static;
use spdk_sys::{
    spdk_bdev_module, spdk_bdev_module_examine_done, spdk_bdev_module_list_add,
};

use crate::bdev::{
    nexus::{
        nexus_bdev::{Nexus, NexusState},
        nexus_io::NioCtx,
    },
    Bdev,
};
use std::{cell::UnsafeCell, ffi::CString};

const NEXUS_NAME: &str = "NEXUS_CAS_MODULE";
lazy_static! {
    pub(crate) static ref NEXUS_MODULE: NexusModule = NexusModule::new();
    static ref NEXUS_INSTANCES: NexusInstances = NexusInstances {
        inner: UnsafeCell::new(Vec::new()),
    };
}

#[derive(Default, Debug)]
pub struct NexusInstances {
    inner: UnsafeCell<Vec<Box<Nexus>>>,
}

#[derive(Default, Debug)]
pub struct NexusModule {
    /// inner reference to the bdev module that is registered to SPDK
    /// note that SPDK changes this module internally
    pub(crate) module: spdk_bdev_module,
}

unsafe impl Sync for NexusModule {}
unsafe impl Sync for NexusInstances {}

unsafe impl Send for NexusModule {}
unsafe impl Send for NexusInstances {}

impl From<*mut spdk_bdev_module> for NexusModule {
    // cant silence
    #![allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from(m: *mut spdk_bdev_module) -> Self {
        NexusModule {
            module: unsafe { *m },
        }
    }
}
impl NexusModule {
    /// construct a new NexusModule instance and setup main properties
    /// as well as the function table and json-rpc methods if any
    pub fn new() -> Self {
        let mut module: spdk_bdev_module = Default::default();
        module.name = c_str!(NEXUS_NAME);

        module.async_init = false;
        module.async_fini = false;
        module.module_init = Some(Self::nexus_mod_init);
        module.module_fini = Some(Self::nexus_mod_fini);
        module.get_ctx_size = Some(Self::nexus_ctx_size);
        module.examine_config = Some(Self::examine);
        module.examine_disk = None;
        NexusModule { module }
    }

    pub fn as_ptr(&self) -> *mut spdk_bdev_module {
        &self as *const _ as *mut _
    }

    /// obtain a pointer to the raw bdev module
    pub fn current() -> Option<Self> {
        let c_name = std::ffi::CString::new(NEXUS_NAME).unwrap();
        let module =
            unsafe { spdk_sys::spdk_bdev_module_list_find(c_name.as_ptr()) };

        if module.is_null() {
            None
        } else {
            Some(NexusModule::from(module))
        }
    }

    /// return instances
    pub fn get_instances() -> &'static mut Vec<Box<Nexus>> {
        unsafe { &mut (*NEXUS_INSTANCES.inner.get()) }
    }

    /// return the name of the module
    pub fn name(&self) -> String {
        unsafe {
            std::ffi::CStr::from_ptr(self.module.name)
                .to_str()
                .unwrap()
                .to_string()
        }
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
        let _ = unsafe { CString::from_raw(NEXUS_MODULE.module.name as _) };
        Self::get_instances().clear();
    }

    extern "C" fn examine(new_device: *mut spdk_sys::spdk_bdev) {
        let name = Bdev::from(new_device).name();
        let instances = Self::get_instances();

        // dont examine ourselves

        if instances.iter().any(|n| n.name() == name) {
            unsafe {
                spdk_bdev_module_examine_done(
                    &NEXUS_MODULE.module as *const _ as *mut _,
                )
            }
            return;
        }

        instances
            .iter()
            .filter(|n| n.state == NexusState::Init)
            .any(|bdev| {
                let n = unsafe { Nexus::from_raw((*bdev.bdev.inner).ctxt) };
                if n.examine_child(&name) {
                    let _r = n.open();
                    return true;
                }
                false
            });

        unsafe {
            spdk_bdev_module_examine_done(
                &NEXUS_MODULE.module as *const _ as *mut _,
            )
        }
    }

    extern "C" fn nexus_ctx_size() -> i32 {
        std::mem::size_of::<NioCtx>() as i32
    }
}

pub fn register_module() {
    unsafe {
        spdk_bdev_module_list_add((&NEXUS_MODULE.module) as *const _ as *mut _);
    }
}
