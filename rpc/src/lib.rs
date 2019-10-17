extern crate bytes;
extern crate prost;
extern crate prost_derive;
extern crate serde;
extern crate tonic;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
pub mod mayastor {
    include!(concat!(env!("OUT_DIR"), "/mayastor.rs"));
}

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
pub mod service {
    include!(concat!(env!("OUT_DIR"), "/mayastor_service.rs"));
}

pub mod jsonrpc;
