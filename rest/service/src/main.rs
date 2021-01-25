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

    /// Trace rest requests to the Jaeger endpoint agent
    #[structopt(long, short)]
    jaeger: Option<String>,
}

use actix_web_opentelemetry::RequestTracing;
use opentelemetry::{
    global,
    sdk::{propagation::TraceContextPropagator, trace::Tracer},
};
use opentelemetry_jaeger::Uninstall;

fn init_tracing() -> Option<(Tracer, Uninstall)> {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
    if let Some(agent) = CliArgs::from_args().jaeger {
        tracing::info!("Starting jaeger trace pipeline at {}...", agent);
        // Start a new jaeger trace pipeline
        global::set_text_map_propagator(TraceContextPropagator::new());
        let (_tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(agent)
            .with_service_name("rest-server")
            .install()
            .expect("Jaeger pipeline install error");
        Some((_tracer, _uninstall))
    } else {
        None
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // need to keep the jaeger pipeline tracer alive, if enabled
    let _tracer = init_tracing();

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
            .wrap(RequestTracing::new())
            .wrap(middleware::Logger::default())
            .service(v0::nodes::factory())
            .service(v0::pools::factory())
            .service(v0::replicas::factory())
            .service(v0::nexuses::factory())
            .service(v0::children::factory())
            .service(v0::volumes::factory())
    })
    .bind_rustls(CliArgs::from_args().rest, config)?
    .run()
    .await
}
