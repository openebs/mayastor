//! A Registration subsystem is used to keep control-plane in the loop
//! about the lifecycle of mayastor instances.

use spdk_rs::libspdk::{
    spdk_add_subsystem, spdk_subsystem, spdk_subsystem_fini_next, spdk_subsystem_init_next,
};
use std::mem::zeroed;

// wrapper around our Registration subsystem used for registration
pub struct NvmxSubsystem(*mut spdk_subsystem);

use once_cell::sync::OnceCell;
use std::collections::HashMap;

static ADMINQ_POLL_THREADS: OnceCell<HashMap<u32, u64>> = OnceCell::new();

impl Default for NvmxSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl NvmxSubsystem {
    /// Initialise a new subsystem that creates adminq pollers for the nvmx controllers.
    extern "C" fn init() {
        let adminqs = crate::core::Reactors::iter().fold(HashMap::new(), |mut acc, reactor| {
            let core = reactor.core();
            let thread = spdk_rs::Thread::new(format!("nvmx_poll_adminq_{core}"), core)
                .expect("Should be able to allocate the adminq polling threads");
            acc.insert(reactor.core(), thread.id());
            acc
        });

        ADMINQ_POLL_THREADS.get_or_init(|| adminqs);

        unsafe { spdk_subsystem_init_next(0) }
    }

    /// Get the thread id for the adminq poller running on the specified core.
    pub fn adminq_thread_id(core: u32) -> Option<u64> {
        let adminq = ADMINQ_POLL_THREADS.get_or_init(HashMap::new);
        adminq.get(&core).cloned()
    }

    /// Get the [`spdk_rs::Thread`] for the adminq poller running on the specified core.
    pub fn adminq_thread(core: u32) -> Option<spdk_rs::Thread> {
        Self::adminq_thread_id(core).and_then(spdk_rs::Thread::by_id)
    }

    extern "C" fn fini() {
        tracing::debug!("mayastor nvmx subsystem fini");
        // we could delete the threads, but probably don't need to do that explicitly...
        unsafe { spdk_subsystem_fini_next() }
    }

    fn new() -> Self {
        tracing::info!("creating mayastor nvmx subsystem...");
        let ss = spdk_subsystem {
            name: b"mayastor_nvmx_registration\x00" as *const u8 as *const libc::c_char,
            init: Some(Self::init),
            fini: Some(Self::fini),
            write_config_json: None,
            tailq: unsafe { zeroed() },
        };
        Self(Box::into_raw(Box::new(ss)))
    }

    /// Register the subsystem with spdk.
    pub(super) fn register() {
        unsafe { spdk_add_subsystem(NvmxSubsystem::new().0) }
    }
}
