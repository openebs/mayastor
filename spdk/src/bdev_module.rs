use crate::ffihelper::IntoCString;
use std::{
    ffi::{CStr, CString},
    ptr::NonNull,
};

use spdk_sys::{
    spdk_bdev_module,
    spdk_bdev_module___bdev_module_internal_fields,
    spdk_bdev_module_list_add,
    spdk_bdev_module_list_find,
    spdk_json_write_ctx,
};

use snafu::Snafu;
use std::marker::PhantomData;

/// Errors for BDEV module API.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum BdevModuleError {
    #[snafu(display("Bdev module '{}' does not exist", name))]
    BdevModuleNotFound { name: String },
}

/// Wrapper for SPDK BDEV module structure.
pub struct BdevModule {
    /// Pointer to SPDK BDEV module structure.
    /// This value is not dropped intentionally in order to prevent
    /// use after free.
    inner: NonNull<spdk_bdev_module>,
}

/// TODO
impl BdevModule {
    /// Returns module's name.
    pub fn name(&self) -> &str {
        unsafe { CStr::from_ptr((*self.as_ptr()).name).to_str().unwrap() }
    }

    /// TODO: a raw pointer to SPDK BDEV module structure.
    pub fn as_ptr(&self) -> *mut spdk_bdev_module {
        self.inner.as_ptr()
    }

    /// TODO
    pub fn find_by_name(mod_name: &str) -> Result<BdevModule, BdevModuleError> {
        let s = mod_name.into_cstring();
        let p = unsafe { spdk_bdev_module_list_find(s.as_ptr()) };
        match NonNull::new(p) {
            Some(inner) => Ok(BdevModule {
                inner,
            }),
            None => Err(BdevModuleError::BdevModuleNotFound {
                name: String::from(mod_name),
            }),
        }
    }
}

/// Implements a `builder` function that returns a Bdev module
pub trait BdevModuleBuild {
    fn builder(mod_name: &str) -> BdevModuleBuilder<Self> {
        BdevModuleBuilder::new(mod_name)
    }
}

/// TODO
pub trait BdevModuleInit {
    /// Called by SPDK during module initialization.
    ///
    /// # Safety
    ///
    /// TODO
    unsafe extern "C" fn raw_module_init() -> i32 {
        Self::module_init()
    }

    fn module_init() -> i32;
}

/// TODO
pub trait BdevModuleFini {
    /// Optionally called by SPDK during module shutdown.
    ///
    /// # Safety
    ///
    /// TODO
    unsafe extern "C" fn raw_module_fini() {
        Self::module_fini()
    }

    fn module_fini();
}

/// TODO
pub trait BdevModuleGetCtxSize {
    /// Optionally called by SPDK during TODO.
    ///
    /// # Safety
    ///
    /// TODO
    unsafe extern "C" fn raw_get_ctx_size() -> i32 {
        Self::get_ctx_size()
    }

    fn get_ctx_size() -> i32;
}

/// TODO
pub trait BdevModuleConfigJson {
    /// Gets raw JSON configuration.
    ///
    /// # Safety
    ///
    /// TODO
    unsafe extern "C" fn raw_config_json(_c: *mut spdk_json_write_ctx) -> i32 {
        Self::config_json()
    }

    fn config_json() -> i32;
}

/// Bdev module configuration builder.
pub struct BdevModuleBuilder<T: ?Sized> {
    name: CString,
    module_init: Option<unsafe extern "C" fn() -> i32>,
    module_fini: Option<unsafe extern "C" fn()>,
    get_ctx_size: Option<unsafe extern "C" fn() -> i32>,
    config_json: Option<unsafe extern "C" fn(*mut spdk_json_write_ctx) -> i32>,
    _t: PhantomData<T>,
}

/// TODO
impl<T: BdevModuleInit> BdevModuleBuilder<T> {
    pub fn with_module_init(mut self) -> Self {
        self.module_init = Some(T::raw_module_init);
        self
    }
}

/// TODO
impl<T: BdevModuleFini> BdevModuleBuilder<T> {
    pub fn with_module_fini(mut self) -> Self {
        self.module_fini = Some(T::raw_module_fini);
        self
    }
}

/// TODO
impl<T: ?Sized> BdevModuleBuilder<T> {
    fn new(mod_name: &str) -> Self {
        Self {
            name: String::from(mod_name).into_cstring(),
            module_init: Some(default_module_init),
            module_fini: None,
            get_ctx_size: None,
            config_json: None,
            _t: Default::default(),
        }
    }

    /// Consumes the builder, builds a bdev module inner representation,
    /// and registers it.
    pub fn register(self) {
        let inner = Box::new(spdk_bdev_module {
            module_init: self.module_init,
            init_complete: None,
            fini_start: None,
            module_fini: self.module_fini,
            config_json: self.config_json,
            name: self.name.into_raw(),
            get_ctx_size: self.get_ctx_size,
            examine_config: None,
            examine_disk: None,
            async_init: false,
            async_fini: false,
            internal: spdk_bdev_module___bdev_module_internal_fields::default(),
        });

        unsafe { spdk_bdev_module_list_add(Box::into_raw(inner)) }
    }
}

/// Default bdev module initialization routine.
/// SPDK required `module_init` to be provided, even if it does nothing.
unsafe extern "C" fn default_module_init() -> i32 {
    info!("---- default modile init ----");
    0
}
