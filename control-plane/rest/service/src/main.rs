mod authentication;
mod v0;

use actix_service::ServiceFactory;
use actix_web::{
    dev::{MessageBody, ServiceRequest, ServiceResponse},
    middleware,
    App,
    HttpServer,
};

use rustls::{
    internal::pemfile::{certs, rsa_private_keys},
    NoClientAuth,
    ServerConfig,
};
use std::{fs::File, io::BufReader, str::FromStr};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub(crate) struct CliArgs {
    /// The bind address for the REST interface (with HTTPS)
    /// Default: 0.0.0.0:8080
    #[structopt(long, default_value = "0.0.0.0:8080")]
    https: String,
    /// The bind address for the REST interface (with HTTP)
    #[structopt(long)]
    http: Option<String>,
    /// The Nats Server URL or address to connect to
    /// Default: nats://0.0.0.0:4222
    #[structopt(long, short, default_value = "nats://0.0.0.0:4222")]
    nats: String,

    /// Path to the certificate file
    #[structopt(long, short, required_unless = "dummy-certificates")]
    cert_file: Option<String>,
    /// Path to the key file
    #[structopt(long, short, required_unless = "dummy-certificates")]
    key_file: Option<String>,

    /// Use dummy HTTPS certificates (for testing)
    #[structopt(long, short, required_unless = "cert-file")]
    dummy_certificates: bool,

    /// Output the OpenApi specs to this directory
    #[structopt(long, short, parse(try_from_str = parse_dir))]
    output_specs: Option<PathBuf>,

    /// Trace rest requests to the Jaeger endpoint agent
    #[structopt(long, short)]
    jaeger: Option<String>,

    /// Path to JSON Web KEY file used for authenticating REST requests
    #[structopt(long, required_unless = "no-auth")]
    jwk: Option<String>,

    /// Don't authenticate REST requests
    #[structopt(long, required_unless = "jwk")]
    no_auth: bool,
}

fn parse_dir(src: &str) -> anyhow::Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from_str(src)?;
    anyhow::ensure!(path.exists(), "does not exist!");
    anyhow::ensure!(path.is_dir(), "must be a directory!");
    Ok(path)
}

use actix_web_opentelemetry::RequestTracing;
use opentelemetry::{
    global,
    sdk::{propagation::TraceContextPropagator, trace::Tracer},
};
use opentelemetry_jaeger::Uninstall;
use std::path::PathBuf;

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

/// Extension trait for actix-web applications.
pub trait OpenApiExt<T, B> {
    /// configures the App with this version's handlers and openapi generation
    fn configure_api(
        self,
        config: &dyn Fn(actix_web::App<T, B>) -> actix_web::App<T, B>,
    ) -> actix_web::App<T, B>;
}

impl<T, B> OpenApiExt<T, B> for actix_web::App<T, B>
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
    fn configure_api(
        self,
        config: &dyn Fn(actix_web::App<T, B>) -> actix_web::App<T, B>,
    ) -> actix_web::App<T, B> {
        config(self)
    }
}

fn get_certificates() -> anyhow::Result<ServerConfig> {
    if CliArgs::from_args().dummy_certificates {
        get_dummy_certificates()
    } else {
        // guaranteed to be `Some` by the require_unless attribute
        let cert_file = CliArgs::from_args()
            .cert_file
            .expect("cert_file is required");
        let key_file =
            CliArgs::from_args().key_file.expect("key_file is required");
        let cert_file = &mut BufReader::new(File::open(cert_file)?);
        let key_file = &mut BufReader::new(File::open(key_file)?);
        load_certificates(cert_file, key_file)
    }
}

fn get_dummy_certificates() -> anyhow::Result<ServerConfig> {
    let cert_file = &mut BufReader::new(
        &std::include_bytes!("../../certs/rsa/user.chain")[..],
    );
    let key_file = &mut BufReader::new(
        &std::include_bytes!("../../certs/rsa/user.rsa")[..],
    );

    load_certificates(cert_file, key_file)
}

fn load_certificates<R: std::io::Read>(
    cert_file: &mut BufReader<R>,
    key_file: &mut BufReader<R>,
) -> anyhow::Result<ServerConfig> {
    let mut config = ServerConfig::new(NoClientAuth::new());
    let cert_chain = certs(cert_file).map_err(|_| {
        anyhow::anyhow!(
            "Failed to retrieve certificates from the certificate file",
        )
    })?;
    let mut keys = rsa_private_keys(key_file).map_err(|_| {
        anyhow::anyhow!(
            "Failed to retrieve the rsa private keys from the key file",
        )
    })?;
    if keys.is_empty() {
        anyhow::bail!("No keys found in the keys file");
    }
    config.set_single_cert(cert_chain, keys.remove(0))?;
    Ok(config)
}

fn get_jwk_path() -> Option<String> {
    match CliArgs::from_args().jwk {
        Some(path) => Some(path),
        None => match CliArgs::from_args().no_auth {
            true => None,
            false => panic!("Cannot authenticate without a JWK file"),
        },
    }
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // need to keep the jaeger pipeline tracer alive, if enabled
    let _tracer = init_tracing();

    let app = move || {
        App::new()
            .wrap(RequestTracing::new())
            .wrap(middleware::Logger::default())
            .app_data(authentication::init(get_jwk_path()))
            .configure_api(&v0::configure_api)
    };

    if CliArgs::from_args().output_specs.is_some() {
        // call the app which will write out the api specs to files
        let _ = app();
        Ok(())
    } else {
        mbus_api::message_bus_init(CliArgs::from_args().nats).await;
        let server = HttpServer::new(app)
            .bind_rustls(CliArgs::from_args().https, get_certificates()?)?;
        if let Some(http) = CliArgs::from_args().http {
            server.bind(http).map_err(anyhow::Error::from)?
        } else {
            server
        }
        .run()
        .await
        .map_err(|e| e.into())
    }
}
