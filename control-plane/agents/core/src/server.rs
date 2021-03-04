pub mod core;
pub mod node;
pub mod pool;
pub mod volume;

use crate::core::registry;
use common::*;
use mbus_api::v0::ChannelVs;
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
pub(crate) struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    pub(crate) nats: String,

    /// The period at which the registry updates its cache of all
    /// resources from all nodes
    #[structopt(long, short, default_value = "20s")]
    pub(crate) cache_period: humantime::Duration,

    /// Deadline for the mayastor instance keep alive registration
    /// Default: 10s
    #[structopt(long, short, default_value = "10s")]
    pub(crate) deadline: humantime::Duration,
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
    Service::builder(cli_args.nats, ChannelVs::Core)
        .with_default_liveness()
        .connect_message_bus()
        .await
        .with_shared_state(registry::Registry::new(
            CliArgs::from_args().cache_period.into(),
        ))
        .configure(node::configure)
        .configure(pool::configure)
        .configure(volume::configure)
        .run()
        .await;
}

/// Constructs a service handler for `RequestType` which gets redirected to a
/// PoolSvc Handler named `ServiceFnName`
#[macro_export]
macro_rules! impl_request_handler {
    ($RequestType:ident, $ServiceFnName:ident) => {
        /// Needed so we can implement the ServiceSubscriber trait for
        /// the message types external to the crate
        #[derive(Clone, Default)]
        struct ServiceHandler<T> {
            data: PhantomData<T>,
        }
        #[async_trait]
        impl common::ServiceSubscriber for ServiceHandler<$RequestType> {
            async fn handler(
                &self,
                args: common::Arguments<'_>,
            ) -> Result<(), SvcError> {
                let request: ReceivedMessage<$RequestType> =
                    args.request.try_into()?;

                let service: &service::Service = args.context.get_state()?;
                let reply = service.$ServiceFnName(&request.inner()).await?;
                Ok(request.reply(reply).await?)
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$RequestType::default().id()]
            }
        }
    };
}

/// Constructs a service handler for `PublishType` which gets redirected to a
/// PoolSvc Handler named `ServiceFnName`
#[macro_export]
macro_rules! impl_publish_handler {
    ($PublishType:ident, $ServiceFnName:ident) => {
        /// Needed so we can implement the ServiceSubscriber trait for
        /// the message types external to the crate
        #[derive(Clone, Default)]
        struct ServiceHandler<T> {
            data: PhantomData<T>,
        }
        #[async_trait]
        impl common::ServiceSubscriber for ServiceHandler<$PublishType> {
            async fn handler(
                &self,
                args: common::Arguments<'_>,
            ) -> Result<(), SvcError> {
                let request: ReceivedMessage<$PublishType> =
                    args.request.try_into()?;

                let service: &service::Service = args.context.get_state()?;
                service.$ServiceFnName(&request.inner()).await;
                Ok(())
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![$PublishType::default().id()]
            }
        }
    };
}

/// Constructs and calls out to a service handler for `RequestType` which gets
/// redirected to a Service Handler where its name is either:
/// `RequestType` as a snake lowercase (default) or
/// `ServiceFn` parameter (if provided)
#[macro_export]
macro_rules! handler {
    ($RequestType:ident) => {{
        paste::paste! {
            impl_request_handler!(
                $RequestType,
                [<$RequestType:snake:lower>]
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
    ($RequestType:ident, $ServiceFn:ident) => {{
        paste::paste! {
            impl_request_handler!(
                $RequestType,
                $ServiceFn
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
}

/// Constructs and calls out to a service handler for `RequestType` which gets
/// redirected to a Service Handler where its name is either:
/// `RequestType` as a snake lowercase (default) or
/// `ServiceFn` parameter (if provided)
#[macro_export]
macro_rules! handler_publish {
    ($RequestType:ident) => {{
        paste::paste! {
            impl_publish_handler!(
                $RequestType,
                [<$RequestType:snake:lower>]
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
    ($RequestType:ident, $ServiceFn:ident) => {{
        paste::paste! {
            impl_publish_handler!(
                $RequestType,
                $ServiceFn
            );
        }
        ServiceHandler::<$RequestType>::default()
    }};
}
