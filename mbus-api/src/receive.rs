use super::*;

/// Type safe wrapper over a message bus message which decodes the raw
/// message into the actual payload `S` and allows only for a response type `R`.
///
/// # Example:
/// ```
/// let raw_msg = &subscriber.next().await?;
/// let msg: ReceivedMessage<RequestConfig, ReplyConfig> =
///             raw_msg.try_into()?;
///
/// msg.respond(ReplyConfig {}).await.unwrap();
/// // or we can also use the same fn to return an error
/// msg.respond(Err(Error::Message("failure".into()))).await.unwrap();
/// ```
pub struct ReceivedMessage<'a, S, R> {
    request: SendPayload<S>,
    bus_message: &'a BusMessage,
    reply_type: PhantomData<R>,
}

impl<'a, S, R> ReceivedMessage<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    /// Get a clone of the actual payload data which was received.
    pub fn inner(&self) -> S {
        self.request.data.clone()
    }
    /// Get the sender identifier
    pub fn sender(&self) -> SenderId {
        self.request.sender.clone()
    }

    /// Reply back to the sender with the `reply` payload wrapped by
    /// a Result-like type.
    /// May fail if serialization of the reply fails or if the
    /// message bus fails to respond.
    /// Can receive either `R`, `Err()` or `ReplyPayload<R>`.
    pub async fn reply<T: Into<ReplyPayload<R>>>(
        &self,
        reply: T,
    ) -> io::Result<()> {
        let reply: ReplyPayload<R> = reply.into();
        let payload = serde_json::to_vec(&reply)?;
        self.bus_message.respond(&payload).await
    }

    /// Create a new received message object which wraps the send and
    /// receive types around a raw bus message.
    fn new(bus_message: &'a BusMessage) -> Result<Self, io::Error> {
        let request: SendPayload<S> =
            serde_json::from_slice(&bus_message.data)?;
        if request.id == request.data.id() {
            log::info!(
                "We have a message from '{}': {:?}",
                request.sender,
                request.data
            );
            Ok(Self {
                request,
                bus_message,
                reply_type: Default::default(),
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message id!",
            ))
        }
    }
}

/// Message received over the message bus with a reply serialization wrapper
/// For type safety refer to `ReceivedMessage<'a,S,R>`.
pub struct ReceivedRawMessage<'a> {
    bus_msg: &'a BusMessage,
}

impl<'a> ReceivedRawMessage<'a> {
    /// Get a copy of the actual payload data which was sent
    /// May fail if the raw data cannot be deserialized into `S`
    pub fn inner<S: Deserialize<'a>>(&self) -> io::Result<S> {
        let request: SendPayload<S> =
            serde_json::from_slice(&self.bus_msg.data)?;
        Ok(request.data)
    }

    /// Get the identifier of this message.
    /// May fail if the raw data cannot be deserialized into the preamble.
    pub fn id(&self) -> io::Result<MessageId> {
        let preamble: Preamble = serde_json::from_slice(&self.bus_msg.data)?;
        Ok(preamble.id)
    }

    /// Channel where this message traversed
    pub fn channel(&self) -> Channel {
        self.bus_msg.subject.clone().parse().unwrap()
    }

    /// Respond back to the sender with the `reply` payload wrapped by
    /// a Result-like type.
    /// May fail if serialization of the reply fails or if the
    /// message bus fails to respond.
    /// Can receive either `Serialize`, `Err()` or `ReplyPayload<Serialize>`.
    pub async fn respond<T: Serialize, R: Serialize + Into<ReplyPayload<T>>>(
        &self,
        reply: R,
    ) -> io::Result<()> {
        let reply: ReplyPayload<T> = reply.into();
        let payload = serde_json::to_vec(&reply)?;
        self.bus_msg.respond(&payload).await
    }
}

impl<'a> std::convert::From<&'a BusMessage> for ReceivedRawMessage<'a> {
    fn from(value: &'a BusMessage) -> Self {
        Self {
            bus_msg: value,
        }
    }
}

impl<'a, S, R> std::convert::TryFrom<&'a BusMessage>
    for ReceivedMessage<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    type Error = io::Error;

    fn try_from(value: &'a BusMessage) -> Result<Self, Self::Error> {
        ReceivedMessage::<S, R>::new(value)
    }
}

impl<'a, S, R> std::convert::TryFrom<ReceivedRawMessage<'a>>
    for ReceivedMessage<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    type Error = io::Error;

    fn try_from(value: ReceivedRawMessage<'a>) -> Result<Self, Self::Error> {
        ReceivedMessage::<S, R>::new(value.bus_msg)
    }
}

impl<T> From<T> for ReplyPayload<T> {
    fn from(val: T) -> Self {
        ReplyPayload(Ok(val))
    }
}

impl<T> From<Result<T, Error>> for ReplyPayload<T> {
    fn from(val: Result<T, Error>) -> Self {
        ReplyPayload(val)
    }
}
