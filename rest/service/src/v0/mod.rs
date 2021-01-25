//! Version 0 of the URI's
//! Ex: /v0/nodes

pub mod children;
pub mod nexuses;
pub mod nodes;
pub mod pools;
pub mod replicas;
pub mod volumes;

use mbus_api::{
    message_bus::v0::{MessageBus, *},
    v0::Filter,
};
use rest_client::versions::v0::*;

use actix_web::{
    delete,
    dev::{AppService, HttpServiceFactory},
    get,
    put,
    web,
    HttpRequest,
    HttpResponse,
    Responder,
};
