use crate::core::{MayastorEnvironment, ToErrno};
use futures::channel::oneshot;
use snafu::Snafu;
use spdk_rs::{
    ffihelper::{cb_arg, done_errno_cb, drop_cb_arg, ErrnoResult, FfiResult},
    libspdk::{
        spdk_add_subsystem, spdk_add_subsystem_depend, spdk_json_val, spdk_subsystem,
        spdk_subsystem_depend, spdk_subsystem_fini_next, spdk_subsystem_init_next,
        SPDK_JSON_VAL_NAME, SPDK_JSON_VAL_OBJECT_BEGIN, SPDK_JSON_VAL_OBJECT_END,
        SPDK_JSON_VAL_TRUE,
    },
    BdevModule, BdevModuleBuild, WithModuleInit,
};
use std::{ffi::CStr, os::raw::c_void, ptr};

const DISABLE_USER_COPY_KEY: &[u8] = b"disable_user_copy";
const TRUE_VALUE: &[u8] = b"true";
const UBLK_MODULE_NAME: &str = "ublk-module";

/// Ublk module for managing ublk devices.
pub struct UblkModule;

impl WithModuleInit for UblkModule {
    fn module_init() -> i32 {
        0
    }
}

impl BdevModuleBuild for UblkModule {}

impl UblkModule {
    fn register() {
        UblkModule::builder(UBLK_MODULE_NAME)
            .with_module_init()
            .register();
    }

    fn current() -> Result<BdevModule, Error> {
        BdevModule::find_by_name(UBLK_MODULE_NAME).map_err(|_| Error::ModuleNotFound {})
    }

    /// Get the name of the ublk module.
    pub fn name() -> &'static str {
        UBLK_MODULE_NAME
    }
}

fn ublk_create_target_params() -> [spdk_json_val; 4] {
    [
        spdk_json_val {
            start: ptr::null_mut(),
            len: 2,
            type_: SPDK_JSON_VAL_OBJECT_BEGIN,
        },
        spdk_json_val {
            start: DISABLE_USER_COPY_KEY.as_ptr() as *mut c_void,
            len: DISABLE_USER_COPY_KEY.len() as u32,
            type_: SPDK_JSON_VAL_NAME,
        },
        spdk_json_val {
            start: TRUE_VALUE.as_ptr() as *mut c_void,
            len: TRUE_VALUE.len() as u32,
            type_: SPDK_JSON_VAL_TRUE,
        },
        spdk_json_val {
            start: ptr::null_mut(),
            len: 0,
            type_: SPDK_JSON_VAL_OBJECT_END,
        },
    ]
}

pub struct UblkSubsystem(pub(crate) *mut spdk_subsystem);

impl Default for UblkSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl UblkSubsystem {
    extern "C" fn init() {
        tracing::info!("mayastor ublk subsystem ini");
        let args = MayastorEnvironment::global_or_default();
        match args.ublk.enabled.then(|| unsafe {
            let params = ublk_create_target_params();
            spdk_rs::libspdk::ublk_create_target(ptr::null(), params.as_ptr())
        }) {
            None => unsafe { spdk_subsystem_init_next(0) },
            // any situations where we may want to bail out here?
            Some(_) => unsafe { spdk_subsystem_init_next(0) },
        }
    }

    extern "C" fn done_cb(_arg: *mut std::os::raw::c_void) {
        tracing::info!("mayastor ublk subsystem fini done");
        unsafe { spdk_subsystem_fini_next() }
    }

    extern "C" fn fini() {
        tracing::info!("mayastor ublk subsystem fini");
        let args = MayastorEnvironment::global_or_default();
        let next = args.ublk.enabled.then(|| unsafe {
            let result =
                spdk_rs::libspdk::ublk_destroy_target(Some(Self::done_cb), std::ptr::null_mut());
            if result != 0 {
                tracing::error!("Failed to destroy ublk target: {result}");
            }
            result != 0
        });
        if next.unwrap_or(true) {
            unsafe { spdk_subsystem_fini_next() }
        }
    }

    fn new() -> Self {
        info!("creating Mayastor ublk subsystem...");
        let mut ss = Box::<spdk_subsystem>::default();
        ss.name = b"mayastor_grpc_ublk\x00" as *const u8 as *const libc::c_char;
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = None;
        Self(Box::into_raw(ss))
    }

    /// Register the subsystem with spdk.
    pub(super) fn register() {
        UblkModule::register();
        unsafe {
            let mut depend = Box::<spdk_subsystem_depend>::default();
            depend.name = b"mayastor_grpc_ublk\0" as *const u8 as *mut _;
            depend.depends_on = b"ublk\0" as *const u8 as *mut _;
            spdk_add_subsystem(Self::new().0);
            spdk_add_subsystem_depend(Box::into_raw(depend));
        }
    }

    /// Start the ublk disk with the given name.
    pub async fn start_disk<T: spdk_rs::BdevOps>(
        bdev: &crate::core::Bdev<T>,
    ) -> Result<String, Error> {
        let ublk = MayastorEnvironment::global().ublk.clone();
        if !ublk.enabled {
            return Err(Error::NotEnabled {});
        }

        if bdev.is_claimed() {
            // or just return errors?
            if bdev.is_claimed_by(UBLK_MODULE_NAME) {
                if let Some(id) = Self::ublk_id(bdev.name())? {
                    return Ok(Self::uri_(bdev.name(), id));
                }
            }
            return Err(Error::BdevClaim {
                name: bdev.name().to_string(),
            });
        }

        // how to choose the right ublk device id?
        // Should this be driven by control-plane or solely from the io-engine?
        // Device recovery may have some implications on this?
        let ublk_id = 0;

        let cstring = std::ffi::CString::new(bdev.name()).expect("no-mem");
        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
        let sender_arg = cb_arg(sender);
        let rc = unsafe {
            spdk_rs::libspdk::ublk_start_disk(
                cstring.as_ptr(),
                ublk_id,
                ublk.q_count(),
                ublk.q_depth(),
                Some(done_errno_cb),
                sender_arg,
            )
        };

        rc.to_result(|error| {
            drop_cb_arg::<ErrnoResult<()>>(sender_arg);
            Error::Start {
                source: nix::Error::from_raw(error),
                phase: ErrorPhase::Call,
            }
        })?;

        receiver
            .await
            .map_err(|_| Error::Start {
                source: nix::Error::ECANCELED,
                phase: ErrorPhase::Wait,
            })?
            .map_err(|source| Error::Start {
                source,
                phase: ErrorPhase::Callback,
            })?;

        let desc = bdev.open(true).map_err(|source| Error::BdevOpen {
            source: Box::new(source),
        })?;
        if UblkModule::current()?
            .claim_bdev(&desc.bdev(), &desc)
            .is_err()
        {
            let _ = Self::stop_disk_by_id(ublk_id).await;
            return Err(Error::BdevClaim {
                name: bdev.name().to_string(),
            });
        }

        Ok(Self::uri_(bdev.name(), ublk_id))
    }

    async fn stop_disk_by_id(ublk_id: u32) -> Result<(), Error> {
        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
        let sender_arg = cb_arg(sender);
        let rc =
            unsafe { spdk_rs::libspdk::ublk_stop_disk(ublk_id, Some(done_errno_cb), sender_arg) };

        if rc != 0 {
            drop_cb_arg::<ErrnoResult<()>>(sender_arg);
        }

        rc.to_result(|error| Error::Stop {
            source: nix::Error::from_raw(error),
            phase: ErrorPhase::Call,
        })?;

        receiver
            .await
            .map_err(|_| Error::Stop {
                source: nix::Error::ECANCELED,
                phase: ErrorPhase::Wait,
            })?
            .map_err(|source| Error::Stop {
                source,
                phase: ErrorPhase::Callback,
            })
    }

    /// Stop the ublk disk with the given name.
    pub async fn stop_disk<T: spdk_rs::BdevOps>(bdev: &spdk_rs::Bdev<T>) -> Result<(), Error> {
        let Some(ublk_id) = Self::ublk_id(bdev.name())? else {
            return Ok(());
        };

        Self::stop_disk_by_id(ublk_id).await?;

        UblkModule::current()?
            .release_bdev(bdev)
            .map_err(|_| Error::BdevUnclaim {
                name: bdev.name().to_string(),
            })
    }

    /// Get the ublk device id for the given name.
    /// ## Output
    /// Ok(None): ublk device with the given name does not exist.
    /// Ok(Some(id)): ublk device with the given name exists and has the given id.
    /// Err(Error): ublk subsystem is not enabled or any error.
    pub fn ublk_id(name: &str) -> Result<Option<u32>, Error> {
        let env = MayastorEnvironment::global();
        if !env.ublk.enabled {
            return Err(Error::NotEnabled {});
        }

        let mut ublk = unsafe { spdk_rs::libspdk::ublk_dev_first() };
        while !ublk.is_null() {
            let bdev_name = unsafe { spdk_rs::libspdk::ublk_dev_get_bdev_name(ublk) };
            if !bdev_name.is_null() {
                if let Ok(bdev_name) = unsafe { CStr::from_ptr(bdev_name) }.to_str() {
                    if bdev_name == name {
                        return Ok(Some(unsafe { spdk_rs::libspdk::ublk_dev_get_id(ublk) }));
                    }
                }
            }

            ublk = unsafe { spdk_rs::libspdk::ublk_dev_next(ublk) };
        }

        Ok(None)
    }

    fn uri_(name: &str, ublk_id: u32) -> String {
        format!("ublk:///{name}?id={ublk_id}")
    }

    pub fn uri(name: &str) -> Option<String> {
        let ublk_id = Self::ublk_id(name).ok()??;
        Some(Self::uri_(name, ublk_id))
    }
}

/// Ublk subsystem error type.
#[derive(Debug, Clone, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum Error {
    #[snafu(display("ublk subsystem is not enabled"))]
    NotEnabled {},
    #[snafu(display("ublk bdev module is not registered"))]
    ModuleNotFound {},
    #[snafu(display("failed to claim bdev '{name}' for ublk"))]
    BdevClaim {
        name: String,
    },
    #[snafu(display("failed to release ublk claim for bdev '{name}'"))]
    BdevUnclaim {
        name: String,
    },
    #[snafu(display("failed to start ublk disk {phase}: {source}"))]
    Start {
        source: nix::Error,
        phase: ErrorPhase,
    },
    #[snafu(display("failed to stop ublk disk {phase}: {source}"))]
    Stop {
        source: nix::Error,
        phase: ErrorPhase,
    },
    BdevOpen {
        source: Box<crate::core::CoreError>,
    },
}

#[derive(strum_macros::Display, Debug, Clone)]
pub enum ErrorPhase {
    Call,
    Wait,
    Callback,
}

impl ToErrno for Error {
    fn to_errno(&self) -> nix::Error {
        match self {
            Self::NotEnabled {} => nix::Error::ENOTSUP,
            Self::ModuleNotFound {} => nix::Error::ENODEV,
            Self::BdevClaim { .. } => nix::Error::EBUSY,
            Self::BdevUnclaim { .. } => nix::Error::EINVAL,
            Self::Start { source, .. } => *source,
            Self::Stop { source, .. } => *source,
            Self::BdevOpen { source } => source.to_errno(),
        }
    }
}
impl From<Error> for crate::core::CoreError {
    fn from(source: Error) -> Self {
        Self::Ublk { source }
    }
}
