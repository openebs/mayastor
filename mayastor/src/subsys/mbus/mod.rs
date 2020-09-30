//! Message bus connecting mayastor to the control plane components.
//!
//! It is designed to make sending events to control plane easy in the future.
//!
//! A Registration subsystem is used to keep moac in the loop
//! about the lifecycle of mayastor instances.

pub mod mbus_nats;
pub mod registration;

use crate::core::MayastorEnvironment;
use async_trait::async_trait;
use dns_lookup::{lookup_addr, lookup_host};
use mbus_nats::NATS_MSG_BUS;
use registration::Registration;
use serde::Serialize;
use smol::io;
use spdk_sys::{
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
                    s.next().unwrap(),
                    s.next().unwrap().parse::<u16>().expect("Invalid Port"),
                )
            } else {
                (endpoint.as_str(), 4222)
            };

            if let Ok(ipv4) = address_or_ip.parse::<Ipv4Addr>() {
                lookup_addr(&IpAddr::V4(ipv4)).expect("Invalid Ipv4 Address");
            } else {
                lookup_host(&address_or_ip).expect("Invalid Host Name");
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
        debug!("mayastor mbus subsystem init");
        let args = MayastorEnvironment::global_or_default();
        if let (Some(_), Some(grpc)) = (args.mbus_endpoint, args.grpc_endpoint)
        {
            Registration::init(&args.node_name, &grpc.to_string());
        }
        unsafe { spdk_subsystem_init_next(0) }
    }

    extern "C" fn fini() {
        debug!("mayastor mbus subsystem fini");
        let args = MayastorEnvironment::global_or_default();
        if args.mbus_endpoint.is_some() && args.grpc_endpoint.is_some() {
            Registration::get().fini();
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

/// Available Message Bus channels
pub enum Channel {
    /// Registration of mayastor with the control plane
    Register,
    /// DeRegistration of mayastor with the control plane
    DeRegister,
}

impl std::fmt::Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Channel::Register => write!(f, "register"),
            Channel::DeRegister => write!(f, "deregister"),
        }
    }
}

#[async_trait]
pub trait MessageBus {
    /// publish a message - not guaranteed to be sent or received (fire and
    /// forget)
    async fn publish(
        &self,
        channel: Channel,
        message: impl Serialize
            + std::marker::Send
            + std::marker::Sync
            + 'async_trait,
    ) -> std::io::Result<()>;
    /// Send a message and wait for it to be received by the target component
    async fn send(
        &self,
        channel: Channel,
        message: impl Serialize
            + std::marker::Send
            + std::marker::Sync
            + 'async_trait,
    ) -> Result<(), ()>;
    /// Send a message and request a reply from the target component
    async fn request(
        &self,
        channel: Channel,
        message: impl Serialize
            + std::marker::Send
            + std::marker::Sync
            + 'async_trait,
    ) -> Result<Vec<u8>, ()>;
    /// Flush queued messages to the server
    async fn flush(&self) -> io::Result<()>;
}

pub fn message_bus_init() {
    if let Some(nats) = MayastorEnvironment::global_or_default().mbus_endpoint {
        mbus_nats::message_bus_init(nats);
    }
}

pub fn message_bus() -> &'static impl MessageBus {
    NATS_MSG_BUS
        .get()
        .expect("Should be initialised before use")
}
