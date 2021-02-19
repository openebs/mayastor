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

use rest_client::{versions::v0::*, JsonGeneric};

use actix_service::ServiceFactory;
use actix_web::{
    dev::{MessageBody, ServiceRequest, ServiceResponse},
    web::{self, Json},
    HttpRequest,
};
use macros::actix::{delete, get, put};
use paperclip::actix::OpenApiExt;
use std::io::Write;
use structopt::StructOpt;
use tracing::info;

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
        .with_raw_json_spec(|app, spec| {
            if let Some(dir) = super::CliArgs::from_args().output_specs {
                let file = dir.join(&format!("{}_api_spec.json", version()));
                info!("Writing {} to {}", spec_uri(), file.to_string_lossy());
                let mut file = std::fs::File::create(file)
                    .expect("Should create the spec file");
                file.write_all(spec.to_string().as_ref())
                    .expect("Should write the spec to file");
            }
            app
        })
        .with_json_spec_at(&spec_uri())
        .build()
        .configure(swagger_ui::configure)
}
