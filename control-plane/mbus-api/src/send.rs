use super::*;

// todo: replace with proc-macros

/// Main Message trait, which should typically be used to send
/// MessageBus messages.
/// Implements Message trait for the type `S` with the reply type
/// `R`, the message id `I`, the default channel `C`.
/// If specified it makes use of the Request/Publish traits exported
/// by type `T`, otherwise it defaults to using `S`.
/// Also implements the said Request/Publish traits for type `T`, if
/// specified, otherwise it implements them for type `S`.
///
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// bus_impl_message_all!(DummyRequest, DummyId, (), DummyChan);
///
/// let reply = DummyRequest { }.request().await.unwrap();
/// ```
#[macro_export]
macro_rules! bus_impl_message_all {
    ($S:ident, $I:ident, $R:tt, $C:ident) => {
        bus_impl_all!($S, $R);
        bus_impl_message!($S, $I, $R, $C);
    };
    ($S:ident, $I:ident, $R:tt, $C:ident, $T:ident) => {
        bus_impl_all!($T, $S, $R);
        bus_impl_message!($S, $I, $R, $C, $T);
    };
}

/// Implement Request/Reply traits for type `S`.
/// Otherwise, if `T` is specified, then it creates `T` and
/// implements said types for `T`.
/// `S` is the request payload and `R` is the reply payload.
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyReply {}
///
/// bus_impl_all!(DummyRequest,DummyReply);
///
/// let reply = DummyRequest::request(DummyRequest {}, channel, &bus)
///             .await
///             .unwrap();
///
/// bus_impl_all!(Dummy, DummyRequest,DummyReply);
///
/// let reply = Dummy::request(DummyRequest {}, channel, &bus)
///             .await
///             .unwrap();
/// ```
#[macro_export]
macro_rules! bus_impl_all {
    ($S:ident,$R:ty) => {
        bus_impl_request!($S, $R);
        bus_impl_publish!($S);
    };
    ($T:ident,$S:ident,$R:ty) => {
        /// place holder for the message traits, example:
        /// $T::request(..).await
        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct $T {}

        bus_impl_request!($T, $S, $R);
        bus_impl_publish!($T, $S);
    };
}

/// Implement the bus trait for requesting a response back from `T` where
/// `S` is the payload request type and `R` is the reply payload type.
/// Can optionally implement the trait for `S`.
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyReply {}
///
/// bus_impl_request!(DummyRequest,DummyReply);
///
/// let reply = DummyRequest::request(DummyRequest {}, channel, &bus)
///             .await
///             .unwrap();
/// ```
#[macro_export]
macro_rules! bus_impl_request {
    ($S:ident,$R:ty) => {
        impl<'a> MessageRequest<'a, $S, $R> for $S {}
    };
    ($T:ty,$S:ident,$R:ty) => {
        impl<'a> MessageRequest<'a, $S, $R> for $T {}
    };
}

/// Implement the publish bus trait for type `T` which
/// publishes the payload type `S`.
/// Can optionally implement the trait for `S`.
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyPublish {}
///
/// bus_impl_publish!(DummyPublish);
///
/// DummyPublish::request(DummyPublish {}, channel, &bus).await.unwrap()
/// ```
#[macro_export]
macro_rules! bus_impl_publish {
    ($S:ty) => {
        bus_impl_publish!($S, $S);
    };
    ($T:ty,$S:tt) => {
        impl<'a> MessagePublish<'a, $S, ()> for $T {}
    };
}

/// Implement Message trait for the type `S` with the reply type
/// `R`, the message id `I`, the default channel `C`.
/// If specified it makes use of the Request/Publish traits exported
/// by type `T`, otherwise it defaults to using `S`.
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// bus_impl_message!(DummyRequest, DummyId, (), DummyChan);
/// ```
#[macro_export]
macro_rules! bus_impl_message {
    ($S:ident, $I:ident, $R:tt, $C:ident) => {
        bus_impl_message!($S, $I, $R, $C, $S);
    };
    ($S:ident, $I:ident, $R:tt, $C:ident, $T:ident) => {
        #[async_trait::async_trait]
        impl Message for $S {
            type Reply = $R;

            impl_channel_id!($I, $C);

            async fn publish(&self) -> BusResult<()> {
                $T::Publish(self, self.channel(), bus()).await
            }
            async fn request(&self) -> BusResult<$R> {
                $T::Request(self, self.channel(), bus()).await
            }
            async fn request_on<C: Into<Channel> + Send>(
                &self,
                channel: C,
            ) -> BusResult<$R> {
                $T::Request(self, channel.into(), bus()).await
            }
            async fn request_ext(
                &self,
                options: TimeoutOptions,
            ) -> BusResult<$R> {
                $T::Request_Ext(self, self.channel(), bus(), options).await
            }
            async fn request_on_ext<C: Into<Channel> + Send>(
                &self,
                channel: C,
                options: TimeoutOptions,
            ) -> BusResult<$R> {
                $T::Request_Ext(self, channel.into(), bus(), options).await
            }
        }
    };
}

/// Implement request for all objects of `Type`
#[macro_export]
macro_rules! bus_impl_vector_request {
    ($Request:ident, $Inner:ident) => {
        /// Request all the `Inner` elements
        #[derive(Serialize, Deserialize, Default, Debug, Clone)]
        pub struct $Request(pub Vec<$Inner>);
        impl $Request {
            /// returns the first element of the tuple and consumes self
            pub fn into_inner(self) -> Vec<$Inner> {
                self.0
            }
        }
    };
}

/// Trait to send a message `bus` request with the `payload` type `S` via a
/// a `channel` and requesting a response back with the payload type `R`
/// via a specific reply channel.
/// Trait can be implemented using the macro helper `bus_impl_request`.
#[async_trait]
pub trait MessageRequest<'a, S, R>
where
    S: 'a + Sync + Send + Message + Serialize,
    for<'de> R: Deserialize<'de> + Default + 'a + Sync + Send,
{
    /// Sends the message and requests a reply
    /// May fail if the bus fails to publish the message.
    #[allow(non_snake_case)]
    async fn Request<C: Into<Channel> + Send>(
        payload: &'a S,
        channel: C,
        bus: DynBus,
    ) -> BusResult<R> {
        let msg = SendMessage::<S, R>::new(payload, channel.into(), bus);
        msg.request(None).await
    }

    /// Sends the message and requests a reply
    /// May fail if the bus fails to publish the message.
    /// With additional timeout parameters
    #[allow(non_snake_case)]
    async fn Request_Ext(
        payload: &'a S,
        channel: Channel,
        bus: DynBus,
        options: TimeoutOptions,
    ) -> BusResult<R> {
        let msg = SendMessage::<S, R>::new(payload, channel, bus);
        msg.request(Some(options)).await
    }
}

/// Trait to send a message `bus` publish with the `payload` type `S` via a
/// a `channel`. No reply is requested.
/// Trait can be implemented using the macro helper `bus_impl_publish`.
#[async_trait]
pub trait MessagePublish<'a, S, R>
where
    S: 'a + Sync + Send + Message + Serialize,
    for<'de> R: Deserialize<'de> + Default + 'a + Sync + Send,
{
    /// Publishes the Message - not guaranteed to be sent or received (fire and
    /// forget)
    /// May fail if the bus fails to publish the message
    #[allow(non_snake_case)]
    async fn Publish(
        payload: &'a S,
        channel: Channel,
        bus: DynBus,
    ) -> BusResult<()> {
        let msg = SendMessage::<S, R>::new(payload, channel, bus);
        msg.publish().await
    }
}

/// Type specific Message Bus api used to send a message of type `S` over the
/// message bus with an additional type `R` use for request/reply semantics
/// # Example:
/// ```
/// let msg = RequestToSend::<S, R>::new(payload, channel, bus);
///         msg.request().await.unwrap();
/// ```
struct SendMessage<'a, S, R> {
    payload: SendPayload<&'a S>,
    bus: DynBus,
    channel: Channel,
    reply_type: PhantomData<R>,
}

impl<'a, S, R> SendMessage<'a, S, R>
where
    S: Message + Serialize,
    for<'de> R: Deserialize<'de> + 'a,
{
    /// each client needs a unique identification
    /// should this be a creation argument?
    fn name() -> SenderId {
        match std::env::var("NODE_NAME") {
            Ok(val) => val,
            _ => "default".into(),
        }
    }

    /// Creates a new request `Message` with the required payload
    /// using an existing `bus` which is used to sent the payload
    /// via the `channel`.
    pub(crate) fn new(payload: &'a S, channel: Channel, bus: DynBus) -> Self {
        Self {
            payload: SendPayload {
                id: payload.id(),
                data: payload,
                sender: Self::name(),
            },
            reply_type: Default::default(),
            bus,
            channel,
        }
    }

    /// Publishes the Message - not guaranteed to be sent or received (fire and
    /// forget).
    pub(crate) async fn publish(&self) -> BusResult<()> {
        let payload =
            serde_json::to_vec(&self.payload).context(SerializeSend {
                channel: self.channel.clone(),
            })?;
        self.bus.publish(self.channel.clone(), &payload).await
    }

    /// Sends the message and requests a reply.
    pub(crate) async fn request(
        &self,
        options: Option<TimeoutOptions>,
    ) -> BusResult<R> {
        let payload =
            serde_json::to_vec(&self.payload).context(SerializeSend {
                channel: self.channel.clone(),
            })?;
        let reply = self
            .bus
            .request(self.channel.clone(), &payload, options)
            .await?
            .data;
        let reply: ReplyPayload<R> =
            serde_json::from_slice(&reply).context(DeserializeReceive {
                request: serde_json::to_string(&self.payload),
                reply: String::from_utf8(reply),
            })?;
        reply.0.context(ReplyWithError {})
    }
}
