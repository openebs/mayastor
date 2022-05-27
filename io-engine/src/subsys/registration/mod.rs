//! A Registration subsystem is used to keep control-plane in the loop
//! about the lifecycle of mayastor instances.

/// Module for grpc registration implementation
pub mod registration_grpc;

use crate::core::MayastorEnvironment;
use http::Uri;
use registration_grpc::Registration;
use spdk_rs::libspdk::{
    spdk_add_subsystem,
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};
use std::convert::TryFrom;

macro_rules! default_addr {
    () => {
        "https://core"
    };
}
macro_rules! default_port {
    () => {
        50051
    };
}

/// Default grpc server port
pub fn default_port() -> u16 {
    default_port!()
}

/// Default endpoint string - addr:port
pub fn default_endpoint_str() -> &'static str {
    concat!(default_addr!(), ":", default_port!())
}

/// Default endpoint Uri - ip:port
pub fn default_endpoint() -> Uri {
    Uri::try_from(default_endpoint_str()).expect("Expected a valid endpoint")
}

// wrapper around our Registration subsystem used for registration
pub struct RegistrationSubsystem(*mut spdk_subsystem);

impl Default for RegistrationSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistrationSubsystem {
    /// initialise a new subsystem that handles the control plane
    /// registration process
    extern "C" fn init() {
        unsafe { spdk_subsystem_init_next(0) }
    }

    extern "C" fn fini() {
        debug!("mayastor registration subsystem fini");
        let args = MayastorEnvironment::global_or_default();
        if args.grpc_endpoint.is_some() {
            if let Some(registration) = Registration::get() {
                registration.fini();
            }
        }
        unsafe { spdk_subsystem_fini_next() }
    }

    fn new() -> Self {
        info!("creating Mayastor registration subsystem...");
        let mut ss = Box::new(spdk_subsystem::default());
        ss.name = b"mayastor_grpc_registration\x00" as *const u8
            as *const libc::c_char;
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = None;
        Self(Box::into_raw(ss))
    }

    /// register the subsystem with spdk
    pub(super) fn register() {
        unsafe { spdk_add_subsystem(RegistrationSubsystem::new().0) }
    }
}
