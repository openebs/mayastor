#![allow(clippy::field_reassign_with_default)]
//! Version 0 of the URI's
//! Ex: /v0/nodes

pub mod children;
pub mod nexuses;
pub mod nodes;
pub mod pools;
pub mod replicas;
pub mod swagger_ui;
pub mod volumes;

use mbus_api::{
    message_bus::v0::{MessageBus, *},
    v0::Filter,
};
use rest_client::versions::v0::*;

use actix_web::{
    web::{self, Json},
    HttpRequest,
};

use mayastor_macros::actix::{delete, get, put};
