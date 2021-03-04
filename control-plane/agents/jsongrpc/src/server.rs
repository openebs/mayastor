pub mod service;

use async_trait::async_trait;
use common::{errors::SvcError, *};
use mbus_api::{v0::*, *};
use service::*;
use std::{convert::TryInto, marker::PhantomData};
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    nats: String,
}

/// Needed so we can implement the ServiceSubscriber trait for
/// the message types external to the crate
#[derive(Clone, Default)]
struct ServiceHandler<T> {
    data: PhantomData<T>,
}

macro_rules! impl_service_handler {
    // RequestType is the message bus request type
    // ServiceFnName is the name of the service function to route the request
    // into
    ($RequestType:ident, $ServiceFnName:ident) => {
        #[async_trait]
        impl ServiceSubscriber for ServiceHandler<$RequestType> {
            async fn handler(
                &self,
                args: Arguments<'_>,
            ) -> Result<(), SvcError> {
                let request: ReceivedMessage<$RequestType> =
                    args.request.try_into()?;

                let reply =
                    JsonGrpcSvc::$ServiceFnName(&request.inner()).await?;
                Ok(request.reply(reply).await?)
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$RequestType::default().id()]
            }
        }
    };
}

impl_service_handler!(JsonGrpcRequest, json_grpc_call);

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[tokio::main]
async fn main() {
    init_tracing();

    let cli_args = CliArgs::from_args();
    info!("Using options: {:?}", &cli_args);

    server(cli_args).await;
}

async fn server(cli_args: CliArgs) {
    Service::builder(cli_args.nats, ChannelVs::JsonGrpc)
        .connect_message_bus()
        .await
        .with_subscription(ServiceHandler::<JsonGrpcRequest>::default())
        .with_default_liveness()
        .run()
        .await;
}
