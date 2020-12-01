//! Version 0 of the URI's
//! Ex: /v0/nodes

pub mod nodes;
pub mod pools;
pub mod replicas;

use mbus_api::{
    message_bus::v0::{MessageBus, *},
    v0::Filter,
};
use rest_client::versions::v0::*;

use actix_web::{delete, get, put, web, Responder};

use actix_web::dev::{AppService, HttpServiceFactory};
