//!
//! This subsystem should be used to add any specific mayastor functionality.
//!
//! TODO: add config sync
use spdk_sys::{
    spdk_add_subsystem,
    spdk_json_write_ctx,
    spdk_json_write_val_raw,
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};

use crate::jsonrpc::jsonrpc_register;
pub use config::{BaseBdev, Config, NexusBdev, Pool};
use futures::FutureExt;

use crate::bdev::nexus::nexus_bdev::Error;

mod config;
mod opts;

static MAYASTOR_SUBSYS: &str = "mayastor";
pub struct MayastorSubsystem(*mut spdk_subsystem);

impl Default for MayastorSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl MayastorSubsystem {
    extern "C" fn init() {
        debug!("mayastor subsystem init");

        // write the config out to disk where the target is the same as source
        // if no config file is given, simply return Ok().
        jsonrpc_register::<(), _, _, Error>("mayastor_config_export", |_| {
            let f = async move {
                let cfg = Config::by_ref();
                if let Some(target) = cfg.source.as_ref() {
                    if let Err(e) = cfg.write(&target) {
                        error!("error writing config file {} {}", target, e);
                    }
                } else {
                    warn!("request to save config file but no source file was given, guess \
                    you have to scribble it down yourself {}", '\u{1f609}');
                }
                Ok(())
            };

            f.boxed_local()
        });

        unsafe { spdk_subsystem_init_next(0) };
    }

    extern "C" fn fini() {
        debug!("mayastor subsystem fini");
        unsafe { spdk_subsystem_fini_next() };
    }

    extern "C" fn config(w: *mut spdk_json_write_ctx) {
        let data = match serde_json::to_string(Config::by_ref()) {
            Ok(it) => it,
            _ => return,
        };

        unsafe {
            spdk_json_write_val_raw(
                w,
                data.as_ptr() as *const _,
                data.as_bytes().len() as u64,
            );
        }
    }

    pub fn new() -> Self {
        debug!("creating Mayastor subsystem...");
        let mut ss = Box::new(spdk_subsystem::default());
        ss.name = std::ffi::CString::new(MAYASTOR_SUBSYS).unwrap().into_raw();
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = Some(Self::config);

        Self(Box::into_raw(ss))
    }
}

pub(crate) fn register_subsystem() {
    unsafe { spdk_add_subsystem(MayastorSubsystem::new().0) }
}
