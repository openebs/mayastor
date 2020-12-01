mod v0;

use actix_web::{middleware, App, HttpServer};
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
            .service(v0::nodes::factory())
            .service(v0::pools::factory())
            .service(v0::replicas::factory())
    })
    .bind_rustls(CliArgs::from_args().rest, config)?
    .run()
    .await
}
