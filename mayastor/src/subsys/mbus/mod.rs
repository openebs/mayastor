//! Message bus connecting mayastor to the control plane components.
//!
//! It is designed to make sending events to control plane easy in the future.
//!
//! A Registration subsystem is used to keep moac in the loop
//! about the lifecycle of mayastor instances.

pub mod registration;

use crate::core::MayastorEnvironment;
use dns_lookup::{lookup_addr, lookup_host};
use registration::Registration;
use spdk_rs::libspdk::{
    spdk_add_subsystem,
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};
use std::net::{IpAddr, Ipv4Addr};

pub fn mbus_endpoint(endpoint: Option<String>) -> Option<String> {
    match endpoint {
        Some(endpoint) => {
            let (address_or_ip, port) = if endpoint.contains(':') {
                let mut s = endpoint.split(':');
                (
                    s.next().expect("Invalid NATS endpoint"),
                    s.next()
                        .unwrap()
                        .parse::<u16>()
                        .expect("Invalid NATS endpoint port"),
                )
            } else {
                (endpoint.as_str(), 4222)
            };

            debug!("Looking up nats endpoint {}...", address_or_ip);
            if let Ok(ipv4) = address_or_ip.parse::<Ipv4Addr>() {
                let nats = lookup_addr(&IpAddr::V4(ipv4))
                    .expect("Invalid Ipv4 Address");
                debug!("Nats endpoint found at {}", nats);
            } else {
                let nats = lookup_host(address_or_ip)
                    .expect("Failed to lookup the NATS endpoint");
                debug!("Nats endpoint found at {:?}", nats);
            }

            Some(format!("{}:{}", address_or_ip, port))
        }
        _ => None,
    }
}

// wrapper around our MBUS subsystem used for registration
pub struct MessageBusSubsystem(*mut spdk_subsystem);

impl Default for MessageBusSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageBusSubsystem {
    /// initialise a new subsystem that handles the control plane
    /// message bus registration process
    extern "C" fn init() {
        unsafe { spdk_subsystem_init_next(0) }
    }

    extern "C" fn fini() {
        debug!("mayastor mbus subsystem fini");
        let args = MayastorEnvironment::global_or_default();
        if args.mbus_endpoint.is_some() && args.grpc_endpoint.is_some() {
            if let Some(registration) = Registration::get() {
                registration.fini();
            }
        }
        unsafe { spdk_subsystem_fini_next() }
    }

    fn new() -> Self {
        info!("creating Mayastor mbus subsystem...");
        let mut ss = Box::new(spdk_subsystem::default());
        ss.name = b"mayastor_mbus\x00" as *const u8 as *const libc::c_char;
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = None;
        Self(Box::into_raw(ss))
    }

    /// register the subsystem with spdk
    pub(super) fn register() {
        unsafe { spdk_add_subsystem(MessageBusSubsystem::new().0) }
    }
}

pub fn message_bus_init() {
    if let Some(nats) = MayastorEnvironment::global_or_default().mbus_endpoint {
        mbus_api::message_bus_init_tokio(nats);
    }
}
