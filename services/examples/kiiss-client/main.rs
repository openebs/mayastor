use log::info;
use mbus_api::*;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default()
            .filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    client().await;
}

async fn client() {
    let cli_args = CliArgs::from_args();
    mbus_api::message_bus_init(cli_args.url).await;

    ConfigUpdate {
        kind: Config::MayastorConfig,
        data: "My config...".into(),
    }
    .request()
    .await
    .unwrap();

    let config = GetConfig::Request(
        &ConfigGetCurrent {
            kind: Config::MayastorConfig,
        },
        Channel::Kiiss,
        bus(),
    )
    .await
    .unwrap();

    info!(
        "Received config: {:?}",
        std::str::from_utf8(&config.config).unwrap()
    );
}
