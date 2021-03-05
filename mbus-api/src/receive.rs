use super::*;

/// Type safe wrapper over a message bus message which decodes the raw
/// message into the actual payload `S` and allows only for a response type `R`.
///
/// # Example:
/// ```
/// let raw_msg = &subscriber.next().await?;
/// let msg: ReceivedMessageExt<RequestConfig, ReplyConfig> =
///             raw_msg.try_into()?;
///
/// msg.respond(ReplyConfig {}).await.unwrap();
/// // or we can also use the same fn to return an error
/// msg.respond(Err(Error::Message("failure".into()))).await.unwrap();
/// ```
pub struct ReceivedMessageExt<'a, S, R> {
    request: SendPayload<S>,
    bus_message: &'a BusMessage,
    reply_type: PhantomData<R>,
}

/// Specialization of type safe wrapper over a message bus message which decodes
/// the raw message into the actual payload `S` and allows only for a response
/// type `R` which is determined based on `S: Message` as a `Message::Reply`
/// type.
///
/// # Example:
/// ```
/// let raw_msg = &subscriber.next().await?;
/// let msg: ReceivedMessage<RequestConfig> =
///             raw_msg.try_into()?;
///
/// msg.respond(ReplyConfig {}).await.unwrap();
/// // or we can also use the same fn to return an error
/// msg.respond(Err(Error::Message("failure".into()))).await.unwrap();
/// ```
pub type ReceivedMessage<'a, S> =
    ReceivedMessageExt<'a, S, <S as Message>::Reply>;

impl<'a, S, R> ReceivedMessageExt<'a, S, R>
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
    ) -> BusResult<()> {
        let reply: ReplyPayload<R> = reply.into();
        let payload = serde_json::to_vec(&reply).context(SerializeReply {
            request: self.request.id.clone(),
        })?;
        self.bus_message.respond(&payload).await.context(Reply {
            request: self.request.id.clone(),
        })
    }

    /// Create a new received message object which wraps the send and
    /// receive types around a raw bus message.
    fn new(bus_message: &'a BusMessage) -> Result<Self, Error> {
        let request: SendPayload<S> = serde_json::from_slice(&bus_message.data)
            .context(DeserializeSend {
                receiver: std::any::type_name::<S>(),
                payload: String::from_utf8(bus_message.data.clone()),
            })?;
        if request.id == request.data.id() {
            tracing::trace!(
                "Received message from '{}': {:?}",
                request.sender,
                request.data
            );
            Ok(Self {
                request,
                bus_message,
                reply_type: Default::default(),
            })
        } else {
            Err(Error::WrongMessageId {
                received: request.id,
                expected: request.data.id(),
            })
        }
    }
}

/// Message received over the message bus with a reply serialization wrapper
/// For type safety refer to `ReceivedMessage<'a,S,R>`.
#[derive(Clone)]
pub struct ReceivedRawMessage<'a> {
    bus_msg: &'a BusMessage,
}

impl std::fmt::Display for ReceivedRawMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "channel: {}, msg_id: {:?}, reply_id: {:?}, data: {:?}",
            self.bus_msg.subject,
            self.id(),
            self.bus_msg.reply,
            std::str::from_utf8(&self.bus_msg.data)
        )
    }
}

impl<'a> ReceivedRawMessage<'a> {
    /// Get a copy of the actual payload data which was sent
    /// May fail if the raw data cannot be deserialized into `S`
    pub fn inner<S: Deserialize<'a> + Message>(&self) -> BusResult<S> {
        let request: SendPayload<S> = serde_json::from_slice(
            &self.bus_msg.data,
        )
        .context(DeserializeSend {
            receiver: std::any::type_name::<S>(),
            payload: String::from_utf8(self.bus_msg.data.clone()),
        })?;
        Ok(request.data)
    }

    /// Get the identifier of this message.
    /// May fail if the raw data cannot be deserialized into the preamble.
    pub fn id(&self) -> BusResult<MessageId> {
        let preamble: Preamble = serde_json::from_slice(&self.bus_msg.data)
            .context(DeserializeSend {
                receiver: std::any::type_name::<Preamble>(),
                payload: String::from_utf8(self.bus_msg.data.clone()),
            })?;
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
    ) -> BusResult<()> {
        let reply: ReplyPayload<T> = reply.into();
        let payload = serde_json::to_vec(&reply).context(SerializeReply {
            request: self.id()?,
        })?;
        self.bus_msg.respond(&payload).await.context(Reply {
            request: self.id()?,
        })
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
    for ReceivedMessageExt<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    type Error = Error;

    fn try_from(value: &'a BusMessage) -> Result<Self, Self::Error> {
        ReceivedMessageExt::<S, R>::new(value)
    }
}

impl<'a, S, R> std::convert::TryFrom<ReceivedRawMessage<'a>>
    for ReceivedMessageExt<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    type Error = Error;

    fn try_from(value: ReceivedRawMessage<'a>) -> Result<Self, Self::Error> {
        ReceivedMessageExt::<S, R>::new(value.bus_msg)
    }
}

impl<T> From<T> for ReplyPayload<T> {
    fn from(val: T) -> Self {
        ReplyPayload(Ok(val))
    }
}

impl<T> From<Result<T, ReplyError>> for ReplyPayload<T> {
    fn from(val: Result<T, ReplyError>) -> Self {
        ReplyPayload(val)
    }
}
