#![allow(clippy::field_reassign_with_default)]
//! Version 0 of the URI's
//! Ex: /v0/nodes

pub mod block_devices;
pub mod children;
pub mod jsongrpc;
pub mod nexuses;
pub mod nodes;
pub mod pools;
pub mod replicas;
pub mod swagger_ui;
pub mod volumes;

use rest_client::{versions::v0::*, JsonGeneric};

use actix_service::ServiceFactory;
use actix_web::{
    dev::{MessageBody, ServiceRequest, ServiceResponse},
    web::{self, Json},
    HttpRequest,
};
use macros::actix::{delete, get, put};
use paperclip::actix::OpenApiExt;

fn version() -> String {
    "v0".into()
}
fn base_path() -> String {
    format!("/{}", version())
}
fn spec_uri() -> String {
    format!("/{}/api/spec", version())
}
fn get_api() -> paperclip::v2::models::DefaultApiRaw {
    let mut api = paperclip::v2::models::DefaultApiRaw::default();
    api.info.version = version();
    api.info.title = "Mayastor RESTful API".into();
    api.base_path = Some(base_path());
    api
}

fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    nodes::configure(cfg);
    pools::configure(cfg);
    replicas::configure(cfg);
    nexuses::configure(cfg);
    children::configure(cfg);
    volumes::configure(cfg);
    jsongrpc::configure(cfg);
    block_devices::configure(cfg);
}

pub(super) fn configure_api<T, B>(
    api: actix_web::App<T, B>,
) -> actix_web::App<T, B>
where
    B: MessageBody,
    T: ServiceFactory<
        Config = (),
        Request = ServiceRequest,
        Response = ServiceResponse<B>,
        Error = actix_web::Error,
        InitError = (),
    >,
{
    api.wrap_api_with_spec(get_api())
        .configure(configure)
        .with_json_spec_at(&spec_uri())
        .build()
        .configure(swagger_ui::configure)
}
