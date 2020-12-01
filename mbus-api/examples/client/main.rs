use log::info;
use mbus_api::{Message, *};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use structopt::StructOpt;
use tokio::stream::StreamExt;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,

    /// Channel to send to
    #[structopt(long, short, default_value = "v0/default")]
    channel: Channel,

    /// With server in this binary
    #[structopt(long, short)]
    server: bool,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct DummyRequest {}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct DummyReply {
    name: String,
}

// note: in this example we use the default message id
// because we're adding the message types outside of the
// library which should not be done so we have to fake
// out the message id as `Default`.
bus_impl_message_all!(DummyRequest, Default, DummyReply, Default);

async fn start_server_side() {
    let cli_args = CliArgs::from_args();

    let mut sub = bus().subscribe(cli_args.channel).await.unwrap();

    tokio::spawn(async move {
        // server side
        let mut count = 1;
        loop {
            let message = &sub.next().await.unwrap();
            let message: ReceivedRawMessage = message.into();
            message
                .respond(DummyReply {
                    name: format!("example {}", count),
                })
                .await
                .unwrap();
            count += 1;
        }
    });
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default()
            .filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    let cli_args = CliArgs::from_args();
    log::info!("Using args: {:?}", cli_args);

    message_bus_init(cli_args.url).await;

    if cli_args.server {
        // server side needs to subscribe first, unless a streaming model is
        // used
        start_server_side().await;
    }

    let options = TimeoutOptions::new()
        .with_timeout(Duration::from_secs(1))
        .with_max_retries(Some(3));

    // request() will use the bus default timeout and retries
    let reply = DummyRequest {}.request_ext(options).await.unwrap();
    info!("Received reply: {:?}", reply);

    // We can also use the following api to specify a different channel and bus
    let reply = DummyRequest::Request(
        &DummyRequest {},
        Channel::v0(v0::ChannelVs::Default),
        bus(),
    )
    .await
    .unwrap();
    info!("Received reply: {:?}", reply);
}
