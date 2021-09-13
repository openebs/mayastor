use std::{ffi::CString, marker::PhantomData, ptr::NonNull};

use crate::{
    ffihelper::{AsStr, IntoCString},
    Bdev,
    BdevIter,
    BdevOps,
    JsonWriteContext,
    Result,
    SpdkError,
};

use spdk_sys::{
    spdk_bdev_desc,
    spdk_bdev_module,
    spdk_bdev_module___bdev_module_internal_fields,
    spdk_bdev_module_claim_bdev,
    spdk_bdev_module_list_add,
    spdk_bdev_module_list_find,
    spdk_json_write_ctx,
};

/// Wrapper for SPDK Bdev module structure.
pub struct BdevModule {
    /// Pointer to SPDK Bdev module structure.
    /// This value is not dropped intentionally in order to prevent
    /// use after free.
    inner: NonNull<spdk_bdev_module>,
}

impl BdevModule {
    /// Returns module's name.
    pub fn name(&self) -> &str {
        self.as_ref().name.as_str()
    }

    /// Finds a Bdev module by its name.
    ///
    /// # Parameters
    ///
    /// * `mod_name` - Module name to look up by.
    pub fn find_by_name(mod_name: &str) -> Result<BdevModule> {
        let s = mod_name.into_cstring();
        let p = unsafe { spdk_bdev_module_list_find(s.as_ptr()) };
        match NonNull::new(p) {
            Some(inner) => Ok(BdevModule {
                inner,
            }),
            None => Err(SpdkError::BdevModuleNotFound {
                name: String::from(mod_name),
            }),
        }
    }

    /// TODO
    pub fn iter_bdevs<T>(&self) -> BdevIter<T>
    where
        T: BdevOps,
    {
        BdevIter::<T>::new(self)
    }

    /// Lays exclusive write claim to a Bdev.
    ///
    /// # Parameters
    ///
    /// * `bdev` - Block device to be claimed.
    /// * `desc` - Descriptor for the block device.
    pub fn claim_bdev<T>(
        &self,
        bdev: &Bdev<T>,
        desc: *mut spdk_bdev_desc,
    ) -> Result<()>
    where
        T: BdevOps,
    {
        let err = unsafe {
            spdk_bdev_module_claim_bdev(bdev.as_ptr(), desc, self.as_ptr())
        };

        if err == 0 {
            debug!("Claimed Bdev '{}'", bdev.name());
            Ok(())
        } else {
            Err(SpdkError::BdevAlreadyClaimed {
                name: bdev.name().to_string(),
            })
        }
    }

    /// Releases a write claim on a block device by this module.
    ///
    /// # Parameters
    ///
    /// * `bdev` - Block device to be released.
    pub fn release_bdev<T>(&self, bdev: &Bdev<T>) -> Result<()>
    where
        T: BdevOps,
    {
        if bdev.is_claimed_by_module(self) {
            bdev.release_claim();
            Ok(())
        } else {
            Err(SpdkError::BdevNotClaimed {
                name: bdev.name().to_string(),
                mod_name: self.name().to_string(),
            })
        }
    }

    /// Creates a new `spdk_bdev_module` wrapper from an SPDK structure pointer.
    pub(crate) fn from_ptr(ptr: *mut spdk_bdev_module) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
        }
    }

    /// Returns a pointer to the underlying `spdk_bdev_module` structure.
    pub(crate) fn as_ptr(&self) -> *mut spdk_bdev_module {
        self.inner.as_ptr()
    }

    /// Returns a reference to the underlying `spdk_bdev_module` structure.
    pub(crate) fn as_ref(&self) -> &spdk_bdev_module {
        unsafe { self.inner.as_ref() }
    }

    /// `as_ptr` for legacy use.
    /// TODO: remove me.
    pub fn legacy_as_ptr(&self) -> *mut spdk_bdev_module {
        self.as_ptr()
    }
}

/// Implements a `builder()` function that returns a Bdev module builder.
pub trait BdevModuleBuild {
    fn builder(mod_name: &str) -> BdevModuleBuilder<Self> {
        BdevModuleBuilder::new(mod_name)
    }
}

/// Bdev module has to implement this trait in order to enable
/// a module initialization callback.
pub trait WithModuleInit {
    /// TODO
    fn module_init() -> i32;
}

/// Bdev module has to implement this trait in order to enable an optional
/// module shutdown callback.
pub trait WithModuleFini {
    /// TODO
    fn module_fini();
}

/// Bdev module has to implement this trait in order to enable callback
/// that returns context size.
pub trait WithModuleGetCtxSize {
    /// TODO
    fn ctx_size() -> i32;
}

/// TODO
pub trait WithModuleConfigJson {
    /// TODO
    fn config_json(w: JsonWriteContext) -> i32;
}

/// Called by SPDK during module initialization.
///
/// # Safety
///
/// TODO
unsafe extern "C" fn inner_module_init<M>() -> i32
where
    M: WithModuleInit,
{
    M::module_init()
}

/// Default Bdev module initialization routine.
/// SPDK required `module_init` to be provided, even if it does nothing.
unsafe extern "C" fn default_module_init() -> i32 {
    // TODO: good msg here
    info!("---- default module init ----");
    0
}

/// Optionally called by SPDK during module shutdown.
///
/// # Safety
///
/// TODO
unsafe extern "C" fn inner_module_fini<M>()
where
    M: WithModuleFini,
{
    M::module_fini()
}

/// Optionally called by SPDK during TODO.
///
/// # Safety
///
/// TODO
unsafe extern "C" fn inner_get_ctx_size<M>() -> i32
where
    M: WithModuleGetCtxSize,
{
    M::ctx_size()
}

/// Returns raw JSON configuration.
///
/// # Safety
///
/// TODO
unsafe extern "C" fn inner_config_json<M>(w: *mut spdk_json_write_ctx) -> i32
where
    M: WithModuleConfigJson,
{
    M::config_json(JsonWriteContext::from_ptr(w))
}

/// Bdev module configuration builder.
pub struct BdevModuleBuilder<M: ?Sized> {
    name: CString,
    module_init: Option<unsafe extern "C" fn() -> i32>,
    module_fini: Option<unsafe extern "C" fn()>,
    get_ctx_size: Option<unsafe extern "C" fn() -> i32>,
    config_json: Option<unsafe extern "C" fn(*mut spdk_json_write_ctx) -> i32>,
    _module: PhantomData<M>,
}

impl<M> BdevModuleBuilder<M>
where
    M: WithModuleInit,
{
    /// TODO
    pub fn with_module_init(mut self) -> Self {
        self.module_init = Some(inner_module_init::<M>);
        self
    }
}

impl<M> BdevModuleBuilder<M>
where
    M: WithModuleFini,
{
    /// TODO
    pub fn with_module_fini(mut self) -> Self {
        self.module_fini = Some(inner_module_fini::<M>);
        self
    }
}

impl<M> BdevModuleBuilder<M>
where
    M: WithModuleGetCtxSize,
{
    /// TODO
    pub fn with_module_ctx_size(mut self) -> Self {
        self.get_ctx_size = Some(inner_get_ctx_size::<M>);
        self
    }
}

impl<M> BdevModuleBuilder<M>
where
    M: WithModuleConfigJson,
{
    pub fn with_module_config_json(mut self) -> Self {
        self.config_json = Some(inner_config_json::<M>);
        self
    }
}

/// TODO
impl<M: ?Sized> BdevModuleBuilder<M> {
    fn new(mod_name: &str) -> Self {
        Self {
            name: String::from(mod_name).into_cstring(),
            module_init: Some(default_module_init),
            module_fini: None,
            get_ctx_size: None,
            config_json: None,
            _module: Default::default(),
        }
    }

    /// Consumes the builder, builds a new Bdev module inner representation,
    /// and registers it within SPDK.
    /// This new module can be later obtained via `find_by_name()` method
    /// of `BdevModule`.
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
