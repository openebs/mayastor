#[macro_use]
extern crate lazy_static;

use async_trait::async_trait;
use common::*;
use mbus_api::{v0::*, *};
use std::{collections::HashMap, convert::TryInto, marker::PhantomData};
use structopt::StructOpt;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,
}

/// Needed so we can implement the ServiceSubscriber trait for
/// the message types external to the crate
#[derive(Clone, Default)]
struct ServiceHandler<T> {
    data: PhantomData<T>,
}

#[derive(Default)]
struct ConfigState {
    state: Mutex<HashMap<SenderId, HashMap<Config, Vec<u8>>>>,
}

lazy_static! {
    static ref CONFIGS: ConfigState = Default::default();
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<ConfigUpdate> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let data: ConfigUpdate = args.request.inner()?;
        info!("Received: {:?}", data);

        let msg: ReceivedMessageExt<ConfigUpdate, ()> =
            args.request.try_into()?;
        let config = msg.inner();

        let mut state = CONFIGS.state.lock().await;

        match state.get_mut(&msg.sender()) {
            Some(map) => {
                map.insert(config.kind, config.data);
            }
            None => {
                let mut config_map = HashMap::new();
                config_map.insert(config.kind, config.data);
                state.insert(msg.sender(), config_map);
            }
        }

        msg.reply(()).await
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![ConfigUpdate::default().id()]
    }
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<ConfigGetCurrent> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let data: ConfigGetCurrent = args.request.inner()?;
        info!("Received: {:?}", data);

        let msg: ReceivedMessageExt<ConfigGetCurrent, ReplyConfig> =
            args.request.try_into()?;
        let request = msg.inner();

        let state = CONFIGS.state.lock().await;

        match state.get(&msg.sender()) {
            Some(config) => match config.get(&request.kind) {
                Some(data) => {
                    msg.reply(ReplyConfig {
                        config: data.clone(),
                    })
                    .await
                }
                None => {
                    msg.reply(Err(ReplyError::WithMessage {
                        message: "Config is missing".into(),
                    }))
                    .await
                }
            },
            None => {
                msg.reply(Err(ReplyError::WithMessage {
                    message: "Config is missing".into(),
                }))
                .await
            }
        }
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![ConfigGetCurrent::default().id()]
    }
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<Register> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let _: ReceivedMessageExt<Register, ()> = args.request.try_into()?;
        Ok(())
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![Register::default().id()]
    }
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<Deregister> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let _: ReceivedMessageExt<Deregister, ()> = args.request.try_into()?;
        Ok(())
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![Deregister::default().id()]
    }
}

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
    Service::builder(cli_args.url, ChannelVs::Kiiss)
        .with_subscription(ServiceHandler::<ConfigUpdate>::default())
        .with_subscription(ServiceHandler::<ConfigGetCurrent>::default())
        .with_channel(ChannelVs::Registry)
        .with_subscription(ServiceHandler::<Register>::default())
        .with_subscription(ServiceHandler::<Deregister>::default())
        .run()
        .await;
}
