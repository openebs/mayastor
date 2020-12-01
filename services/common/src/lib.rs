#![warn(missing_docs)]
//! Control Plane Services library with emphasis on the message bus interaction.
//!
//! It's meant to facilitate the creation of services with a helper builder to
//! subscribe handlers for different message identifiers.

use async_trait::async_trait;
use dyn_clonable::clonable;
use futures::{future::join_all, stream::StreamExt};
use mbus_api::*;
use snafu::{OptionExt, ResultExt, Snafu};
use state::Container;
use std::{collections::HashMap, convert::Into, ops::Deref};
use tracing::{debug, error};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("Channel {} has been closed.", channel.to_string()))]
    GetMessage {
        channel: Channel,
    },
    #[snafu(display("Failed to subscribe on Channel {}", channel.to_string()))]
    Subscribe {
        channel: Channel,
        source: Error,
    },
    GetMessageId {
        channel: Channel,
        source: Error,
    },
    FindSubscription {
        channel: Channel,
        id: MessageId,
    },
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
    channel: Channel,
    subscriptions: HashMap<String, Vec<Box<dyn ServiceSubscriber>>>,
    shared_state: std::sync::Arc<state::Container>,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            server: "".to_string(),
            channel: Default::default(),
            subscriptions: Default::default(),
            shared_state: std::sync::Arc::new(Container::new()),
        }
    }
}

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
        self.state
            .try_get()
            .expect("Requested data type not shared via with_shared_data!")
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
            channel: channel.into(),
            ..Default::default()
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
        if !self.shared_state.set(state) {
            panic!(format!(
                "Shared state for type '{}' has already been set!",
                type_name
            ));
        }
        self
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
                error!("Error processing message: {}", error);
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

        let result =
            subscription
                .handler(arguments)
                .await
                .context(HandleMessage {
                    channel: channel.clone(),
                    id: id.clone(),
                });

        if let Err(error) = result.as_ref() {
            // todo: should an error be returned to the sender?
            error!(
                "Error handling message id {:?}: {:?}",
                subscription.filter(),
                error
            );
        }

        result
    }

    /// Runs the server which services all subscribers asynchronously until all
    /// subscribers are closed
    ///
    /// subscribers are sorted according to the channel they subscribe on
    /// each channel benefits from a tokio thread which routes messages
    /// accordingly todo: only one subscriber per message id supported at
    /// the moment
    pub async fn run(&self) {
        let mut threads = vec![];
        // todo: parse connection options when nats has better support for it
        mbus_api::message_bus_init(self.server.clone()).await;
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
