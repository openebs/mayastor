use async_trait::async_trait;
use common::*;
use mbus_api::*;
use serde::{Deserialize, Serialize};
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,

    /// Act as a Server or a test client
    #[structopt(long, short)]
    client: bool,
}

/// Needed so we can implement the ServiceSubscriber trait for
/// the message types external to the crate
#[derive(Clone, Default)]
struct ServiceHandler<T> {
    data: PhantomData<T>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct GetSvcName {}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct SvcName(String);

bus_impl_message_all!(GetSvcName, Default, SvcName, Default);

#[async_trait]
impl ServiceSubscriber for ServiceHandler<GetSvcName> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let msg: ReceivedMessage<GetSvcName> = args.request.try_into()?;

        let reply = SvcName("example".into());

        println!("Received {:?} and replying {:?}", msg.inner(), reply);

        msg.reply(reply).await
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![GetSvcName::default().id()]
    }
}

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::from_args();

    if cli_args.client {
        client().await;
    } else {
        server().await;
    }
}

async fn client() {
    let cli_args = CliArgs::from_args();
    message_bus_init(cli_args.url).await;

    let svc_name = GetSvcName {}.request().await.unwrap().0;
    println!("Svc Name: {}", svc_name);
}

async fn server() {
    let cli_args = CliArgs::from_args();

    Service::builder(cli_args.url, v0::ChannelVs::Default)
        .with_subscription(ServiceHandler::<GetSvcName>::default())
        .run()
        .await;
}
