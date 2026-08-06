//! Core NATS publishing: the [`NatsPublish`] policy and its live [`NatsPublisher`].

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_nats::Client;
use bytes::Bytes;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedNatsBroker, NatsConnection};
use crate::{convert::headers_to_nats, error::NatsError};

use self::sealed::Sealed;

mod sealed {
    /// Seals [`NatsPublishPolicy`](super::NatsPublishPolicy): pairing a NATS publisher is
    /// synchronous and infallible for both of this crate's policies, and the synchronous
    /// [`publisher`](crate::ConnectedNatsBroker::publisher) accessor depends on that.
    pub trait Sealed {}

    impl Sealed for super::NatsPublish {}
    impl Sealed for crate::jetstream::JetStreamPublish {}
}

/// A publish policy that pairs with a connected NATS broker without I/O.
///
/// Both NATS policies hold nothing but publish options, so bringing one alive is a constructor
/// call rather than broker work. That is what lets
/// [`ConnectedNatsBroker::publisher`](crate::ConnectedNatsBroker::publisher) be synchronous;
/// [`PublishPolicy::pair`], the framework-side entry point, delegates here.
pub trait NatsPublishPolicy: PublishPolicy<ConnectedNatsBroker> + Sealed {
    /// Pairs the policy with the connected broker, producing the live publisher.
    #[must_use]
    fn bind(self, connected: &ConnectedNatsBroker) -> Self::Live;
}

/// The Core NATS publish policy: pure declaration, constructible anywhere.
///
/// Core NATS publishing carries no per-publisher options (subject and headers travel with each
/// message), so the policy is a unit marker. It pairs into [`NatsPublisher`], which also serves
/// the [`RequestReply`](ruststream::RequestReply) capability, and it is the broker's
/// [`DefaultPublish`](ruststream::DefaultPublish) policy, so a `publish("subject")` handler
/// mounted without an explicit publisher replies through it.
///
/// # Examples
///
/// ```
/// use ruststream_nats::NatsPublish;
///
/// let policy = NatsPublish;
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct NatsPublish;

impl PublishPolicy<ConnectedNatsBroker> for NatsPublish {
    type Live = NatsPublisher;

    async fn pair(self, connected: &ConnectedNatsBroker) -> Result<Self::Live, PairError> {
        Ok(self.bind(connected))
    }
}

impl NatsPublishPolicy for NatsPublish {
    fn bind(self, connected: &ConnectedNatsBroker) -> Self::Live {
        NatsPublisher::new(Arc::clone(connected.connection()))
    }
}

/// The live Core NATS publisher. Cheap to clone.
///
/// Exists only from a [`ConnectedNatsBroker`], so it always has a connection. It aliases that
/// connection, though, and may outlive it: after the broker shuts down every publish reports
/// [`NatsError::Closed`] instead of silently succeeding against a dead connection.
#[derive(Clone)]
pub struct NatsPublisher {
    connection: Arc<NatsConnection>,
}

impl Debug for NatsPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsPublisher").finish_non_exhaustive()
    }
}

impl NatsPublisher {
    pub(crate) const fn new(connection: Arc<NatsConnection>) -> Self {
        Self { connection }
    }

    pub(crate) fn client_for(&self, subject: &str) -> Result<Client, NatsError> {
        self.connection.live_client(subject).cloned()
    }
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    /// # Cancel safety
    ///
    /// Core NATS publishing is fire-and-forget: the message is handed to the connection's writer
    /// without waiting for the server. Dropping the future may leave the message either sent or
    /// unsent, with no way to tell which.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let client = self.client_for(msg.name())?;
        let subject = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let result = match headers_to_nats(msg.headers()) {
            Some(headers) => client.publish_with_headers(subject, headers, payload).await,
            None => client.publish(subject, payload).await,
        };
        result.map_err(|err| NatsError::Publish(Box::new(err)))
    }
}
