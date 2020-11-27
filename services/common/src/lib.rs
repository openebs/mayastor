#![warn(missing_docs)]
//! Control Plane Services library with emphasis on the message bus interaction.
//!
//! It's meant to facilitate the creation of services with a helper builder to
//! subscribe handlers for different message identifiers.

use async_trait::async_trait;
use dyn_clonable::clonable;
use futures::{future::join_all, stream::StreamExt};
use mbus_api::{v0::Liveness, *};
use snafu::{OptionExt, ResultExt, Snafu};
use state::Container;
use std::{
    collections::HashMap,
    convert::{Into, TryInto},
    ops::Deref,
};
use tracing::{debug, error};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("Channel '{}' has been closed.", channel.to_string()))]
    GetMessage { channel: Channel },
    #[snafu(display("Failed to subscribe on Channel '{}'", channel.to_string()))]
    Subscribe { channel: Channel, source: Error },
    #[snafu(display("Failed to get message Id on Channel '{}'", channel.to_string()))]
    GetMessageId { channel: Channel, source: Error },
    #[snafu(display("Failed to find subscription '{}' on Channel '{}'", id.to_string(), channel.to_string()))]
    FindSubscription { channel: Channel, id: MessageId },
    #[snafu(display("Failed to handle message id '{}' on Channel '{}'", id.to_string(), channel.to_string()))]
    HandleMessage {
        channel: Channel,
        id: MessageId,
        source: Error,
    },
}

/// Runnable service with N subscriptions which listen on a given
/// message bus channel on a specific ID
pub struct Service {
    server: String,
    server_connected: bool,
    channel: Channel,
    subscriptions: HashMap<String, Vec<Box<dyn ServiceSubscriber>>>,
    shared_state: std::sync::Arc<state::Container>,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            server: "".to_string(),
            server_connected: false,
            channel: Default::default(),
            subscriptions: Default::default(),
            shared_state: std::sync::Arc::new(Container::new()),
        }
    }
}

#[derive(Clone)]
/// Service Arguments for the service handler callback
pub struct Arguments<'a> {
    /// Service context, like access to the message bus
    pub context: &'a Context<'a>,
    /// Access to the actual message bus request
    pub request: Request<'a>,
}

impl<'a> Arguments<'a> {
    /// Returns a new Service Argument to be use by a Service Handler
    pub fn new(context: &'a Context, msg: &'a BusMessage) -> Self {
        Self {
            context,
            request: msg.into(),
        }
    }
}

/// Service handling context
/// the message bus which triggered the service callback
#[derive(Clone)]
pub struct Context<'a> {
    bus: &'a DynBus,
    state: &'a Container,
}

impl<'a> Context<'a> {
    /// create a new context
    pub fn new(bus: &'a DynBus, state: &'a Container) -> Self {
        Self {
            bus,
            state,
        }
    }
    /// get the message bus from the context
    pub fn get_bus_as_ref(&self) -> &'a DynBus {
        self.bus
    }
    /// get the shared state of type `T` from the context
    pub fn get_state<T: Send + Sync + 'static>(&self) -> &T {
        match self.state.try_get() {
            Some(state) => state,
            None => {
                let type_name = std::any::type_name::<T>();
                let error = format!(
                    "Requested data type '{}' not shared via with_shared_data",
                    type_name
                );
                panic!(error);
            }
        }
    }
}

/// Service Request received via the message bus
pub type Request<'a> = ReceivedRawMessage<'a>;

#[async_trait]
#[clonable]
/// Trait which must be implemented by each subscriber with the handler
/// which processes the messages and a filter to match message types
pub trait ServiceSubscriber: Clone + Send + Sync {
    /// async handler which processes the messages
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error>;
    /// filter which identifies which messages may be routed to the handler
    fn filter(&self) -> Vec<MessageId>;
}

impl Service {
    /// Setup default service connecting to `server` on subject `channel`
    pub fn builder(server: String, channel: impl Into<Channel>) -> Self {
        Self {
            server,
            server_connected: false,
            channel: channel.into(),
            ..Default::default()
        }
    }

    /// Connect to the provided message bus server immediately
    /// Useful for when dealing with async shared data which might required the
    /// message bus before the builder is complete
    pub async fn connect(mut self) -> Self {
        self.message_bus_init().await;
        self
    }

    async fn message_bus_init(&mut self) {
        if !self.server_connected {
            // todo: parse connection options when nats has better support
            mbus_api::message_bus_init(self.server.clone()).await;
            self.server_connected = true;
        }
    }

    /// Setup default `channel` where `with_subscription` will listen on
    pub fn with_channel(mut self, channel: impl Into<Channel>) -> Self {
        self.channel = channel.into();
        self
    }

    /// Add a new service-wide shared state which can be retried in the handlers
    /// (more than one type of data can be added).
    /// The type must be `Send + Sync + 'static`.
    ///
    /// Example:
    /// # async fn main() {
    /// Service::builder(cli_args.url, Channel::Registry)
    ///         .with_shared_state(NodeStore::default())
    ///         .with_shared_state(More {})
    ///         .with_subscription(ServiceHandler::<Register>::default())
    ///         .run().await;
    ///
    /// # async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
    ///    let store: &NodeStore = args.context.get_state();
    ///    let more: &More = args.context.get_state();
    /// # Ok(())
    /// # }
    pub fn with_shared_state<T: Send + Sync + 'static>(self, state: T) -> Self {
        let type_name = std::any::type_name::<T>();
        tracing::debug!("Adding shared type: {}", type_name);
        if !self.shared_state.set(state) {
            panic!(format!(
                "Shared state for type '{}' has already been set!",
                type_name
            ));
        }
        self
    }

    /// Add a default liveness endpoint which can be used to probe
    /// the service for liveness on the current selected channel.
    ///
    /// Example:
    /// # async fn main() {
    /// Service::builder(cli_args.url, ChannelVs::Node)
    ///         .with_default_liveness()
    ///         .with_subscription(ServiceHandler::<GetNodes>::default())
    ///         .run().await;
    ///
    /// # async fn alive() -> bool {
    ///    Liveness{}.request().await.is_ok()
    /// # }
    pub fn with_default_liveness(self) -> Self {
        #[derive(Clone, Default)]
        struct ServiceHandler<T> {
            data: std::marker::PhantomData<T>,
        }

        #[async_trait]
        impl ServiceSubscriber for ServiceHandler<Liveness> {
            async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
                let request: ReceivedMessage<Liveness> =
                    args.request.try_into()?;
                request.reply(()).await
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![Liveness::default().id()]
            }
        }

        self.with_subscription(ServiceHandler::<Liveness>::default())
    }

    /// Add a new subscriber on the default channel
    pub fn with_subscription(
        self,
        service_subscriber: impl ServiceSubscriber + 'static,
    ) -> Self {
        let channel = self.channel.clone();
        self.with_subscription_channel(channel, service_subscriber)
    }

    /// Add a new subscriber on the given `channel`
    pub fn with_subscription_channel(
        mut self,
        channel: Channel,
        service_subscriber: impl ServiceSubscriber + 'static,
    ) -> Self {
        match self.subscriptions.get_mut(&channel.to_string()) {
            Some(entry) => {
                entry.push(Box::from(service_subscriber));
            }
            None => {
                self.subscriptions.insert(
                    channel.to_string(),
                    vec![Box::from(service_subscriber)],
                );
            }
        };
        self
    }

    async fn run_channel(
        bus: DynBus,
        channel: Channel,
        subscriptions: &[Box<dyn ServiceSubscriber>],
        state: std::sync::Arc<Container>,
    ) -> Result<(), ServiceError> {
        let mut handle =
            bus.subscribe(channel.clone()).await.context(Subscribe {
                channel: channel.clone(),
            })?;

        loop {
            let message = handle.next().await.context(GetMessage {
                channel: channel.clone(),
            })?;

            let context = Context::new(&bus, state.deref());
            let args = Arguments::new(&context, &message);
            debug!("Processing message: {{ {} }}", args.request);

            if let Err(error) =
                Self::process_message(args, &subscriptions).await
            {
                error!("Error processing message: {}", error.full_string());
            }
        }
    }

    async fn process_message(
        arguments: Arguments<'_>,
        subscriptions: &[Box<dyn ServiceSubscriber>],
    ) -> Result<(), ServiceError> {
        let channel = arguments.request.channel();
        let id = &arguments.request.id().context(GetMessageId {
            channel: channel.clone(),
        })?;

        let subscription = subscriptions
            .iter()
            .find(|&subscriber| {
                subscriber.filter().iter().any(|find_id| find_id == id)
            })
            .context(FindSubscription {
                channel: channel.clone(),
                id: id.clone(),
            })?;

        let result = subscription.handler(arguments.clone()).await;

        Self::assess_handler_error(&result, &arguments).await;

        result.context(HandleMessage {
            channel: channel.clone(),
            id: id.clone(),
        })
    }

    async fn assess_handler_error(
        result: &Result<(), Error>,
        arguments: &Arguments<'_>,
    ) {
        if let Err(error) = result.as_ref() {
            match error {
                Error::DeserializeSend {
                    ..
                } => {
                    arguments
                        .request
                        .respond::<(), _>(Err(ReplyError::DeserializeReq {
                            message: error.full_string(),
                        }))
                        .await
                }
                _ => {
                    arguments
                        .request
                        .respond::<(), _>(Err(ReplyError::Process {
                            message: error.full_string(),
                        }))
                        .await
                }
            }
            .ok();
        }
    }

    /// Runs the server which services all subscribers asynchronously until all
    /// subscribers are closed
    ///
    /// subscribers are sorted according to the channel they subscribe on
    /// each channel benefits from a tokio thread which routes messages
    /// accordingly todo: only one subscriber per message id supported at
    /// the moment
    pub async fn run(&mut self) {
        let mut threads = vec![];

        self.message_bus_init().await;
        let bus = mbus_api::bus();

        for subscriptions in self.subscriptions.iter() {
            let bus = bus.clone();
            let channel = subscriptions.0.clone();
            let subscriptions = subscriptions.1.clone();
            let state = self.shared_state.clone();

            let handle = tokio::spawn(async move {
                Self::run_channel(
                    bus,
                    channel.parse().unwrap(),
                    &subscriptions,
                    state,
                )
                .await
            });

            threads.push(handle);
        }

        join_all(threads)
            .await
            .iter()
            .for_each(|result| match result {
                Err(error) => error!("Failed to wait for thread: {:?}", error),
                Ok(Err(error)) => {
                    error!("Error running channel thread: {:?}", error)
                }
                _ => {}
            });
    }
}
