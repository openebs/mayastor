use mbus_api::*;
use serde::{Deserialize, Serialize};
use std::{convert::TryInto, str::FromStr};
use structopt::StructOpt;
use tokio::stream::StreamExt;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,

    /// Channel to listen on
    #[structopt(long, short, default_value = "v0/default")]
    channel: Channel,

    /// Receiver version
    #[structopt(long, short, default_value = "1")]
    version: Version,
}

#[derive(Clone, Debug)]
enum Version {
    V1,
    V2,
    V3,
}

impl FromStr for Version {
    type Err = String;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "1" => Ok(Self::V1),
            "2" => Ok(Self::V2),
            "3" => Ok(Self::V3),
            _ => Err(format!("Could not parse the version: {}", source)),
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Version::V1
    }
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

#[tokio::main]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default()
            .filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    let cli_args = CliArgs::from_args();
    log::info!("Using args: {:?}", cli_args);
    log::info!("CH: {}", Channel::v0(v0::ChannelVs::Default).to_string());

    message_bus_init(cli_args.url).await;

    let mut sub = bus().subscribe(cli_args.channel).await.unwrap();

    let mut count = 1;
    loop {
        match cli_args.version {
            Version::V1 => receive_v1(&mut sub, count).await,
            Version::V2 => receive_v2(&mut sub, count).await,
            Version::V3 => receive_v3(&mut sub, count).await,
        }
        count += 1;
    }
}

async fn receive_v1(sub: &mut nats::asynk::Subscription, count: u64) {
    let message = &sub.next().await.unwrap();
    let message: ReceivedRawMessage = message.into();
    // notice that there is no type validation until we
    // use something like:
    // let data: DummyRequest = message.payload().unwrap();
    message
        .respond(DummyReply {
            name: format!("example {}", count),
        })
        .await
        .unwrap();
}

async fn receive_v2(sub: &mut nats::asynk::Subscription, count: u64) {
    let message = &sub.next().await.unwrap();
    // notice that try_into can fail if the received type does not
    // match the received message
    let message: ReceivedMessageExt<DummyRequest, DummyReply> =
        message.try_into().unwrap();
    message
        .reply(DummyReply {
            name: format!("example {}", count),
        })
        .await
        .unwrap();
}

async fn receive_v3(sub: &mut nats::asynk::Subscription, count: u64) {
    let message = &sub.next().await.unwrap();
    let message: ReceivedMessageExt<DummyRequest, DummyReply> =
        message.try_into().unwrap();
    message
        // same function can receive an error
        .reply(Err(ReplyError {
            kind: ReplyErrorKind::WithMessage,
            resource: ResourceKind::Unknown,
            source: "".to_string(),
            extra: format!("Fake Error {}", count),
        }))
        .await
        .unwrap();
}
