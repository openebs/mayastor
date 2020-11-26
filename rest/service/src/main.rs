use mbus_api::message_bus::v0::{MessageBus, *};

use actix_web::{
    get,
    middleware,
    web,
    App,
    HttpResponse,
    HttpServer,
    Responder,
};
use rustls::{
    internal::pemfile::{certs, rsa_private_keys},
    NoClientAuth,
    ServerConfig,
};
use std::io::BufReader;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Rest Server address to bind to
    /// Default: 0.0.0.0:8080
    #[structopt(long, short, default_value = "0.0.0.0:8080")]
    rest: String,
    /// The Nats Server URL or address to connect to
    /// Default: nats://0.0.0.0:4222
    #[structopt(long, short, default_value = "nats://0.0.0.0:4222")]
    nats: String,
}

#[get("/v0/nodes")]
async fn get_nodes() -> impl Responder {
    match MessageBus::get_nodes().await {
        Ok(nodes) => HttpResponse::Ok().json(nodes),
        Err(error) => {
            let error = serde_json::json!({"error": error.to_string()});
            HttpResponse::InternalServerError().json(error)
        }
    }
}

#[get("/v0/nodes/{id}")]
async fn get_node(web::Path(node_id): web::Path<String>) -> impl Responder {
    match MessageBus::get_node(node_id).await {
        Ok(Some(node)) => HttpResponse::Ok().json(node),
        Ok(None) => HttpResponse::NoContent().json(()),
        Err(error) => {
            let error = serde_json::json!({"error": error.to_string()});
            HttpResponse::InternalServerError().json(error)
        }
    }
}

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    init_tracing();
    mbus_api::message_bus_init(CliArgs::from_args().nats).await;

    // dummy certificates
    let mut config = ServerConfig::new(NoClientAuth::new());
    let cert_file = &mut BufReader::new(
        &std::include_bytes!("../../certs/rsa/user.chain")[..],
    );
    let key_file = &mut BufReader::new(
        &std::include_bytes!("../../certs/rsa/user.rsa")[..],
    );
    let cert_chain = certs(cert_file).unwrap();
    let mut keys = rsa_private_keys(key_file).unwrap();
    config.set_single_cert(cert_chain, keys.remove(0)).unwrap();

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .service(get_nodes)
            .service(get_node)
    })
    .bind_rustls(CliArgs::from_args().rest, config)?
    .run()
    .await
}
