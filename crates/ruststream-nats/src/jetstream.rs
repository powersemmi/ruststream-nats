//! `JetStream` publishing: the [`JetStreamPublish`] policy and its live [`JetStreamPublisher`].
//!
//! `JetStream` publishing is a different contract from Core NATS, not a mode of it: the server
//! answers every publish with an acknowledgement, and the publisher may state what it expects the
//! stream to look like. Both live on this pair, so the Core publisher keeps the fire-and-forget
//! shape the transport actually has.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_nats::jetstream::Context;
use async_nats::jetstream::message::PublishMessage;
use bytes::Bytes;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedNatsBroker, NatsConnection};
use crate::publisher::NatsPublishPolicy;
use crate::{convert::headers_to_nats, error::NatsError};

/// The acknowledgement a `JetStream` stream returns for an accepted publish.
pub use async_nats::jetstream::publish::PublishAck;

/// The `JetStream` publish policy: pure declaration, constructible anywhere.
///
/// Beyond naming the transport, the policy carries the stream expectations the server checks
/// before accepting a message. They are per-publisher declarations: a publisher that states
/// `expect_stream("ORDERS")` fails loudly if its subject is routed to another stream, instead of
/// writing somewhere unintended. The sequence and message-id expectations serve a single writer
/// enforcing an optimistic-concurrency chain.
///
/// # Examples
///
/// ```
/// use ruststream_nats::JetStreamPublish;
///
/// let policy = JetStreamPublish::default().expect_stream("ORDERS");
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct JetStreamPublish {
    /// Every field is an expectation the server checks before accepting the publish.
    stream: Option<String>,
    last_sequence: Option<u64>,
    last_subject_sequence: Option<u64>,
    last_message_id: Option<String>,
}

impl JetStreamPublish {
    /// Requires the subject to be served by the named stream. The server rejects the publish
    /// otherwise, so a misrouted subject surfaces as an error rather than a silent write.
    pub fn expect_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Requires the stream's last sequence to be `sequence` at the moment of the publish.
    pub const fn expect_last_sequence(mut self, sequence: u64) -> Self {
        self.last_sequence = Some(sequence);
        self
    }

    /// Requires the last sequence *on the published subject* to be `sequence`.
    pub const fn expect_last_subject_sequence(mut self, sequence: u64) -> Self {
        self.last_subject_sequence = Some(sequence);
        self
    }

    /// Requires the stream's last message id (the `Nats-Msg-Id` of the previous publish) to be
    /// `id`.
    pub fn expect_last_message_id(mut self, id: impl Into<String>) -> Self {
        self.last_message_id = Some(id.into());
        self
    }

    /// Applies the declared expectations to one outgoing `JetStream` message.
    fn apply(&self, mut message: PublishMessage) -> PublishMessage {
        if let Some(stream) = &self.stream {
            message = message.expected_stream(stream);
        }
        if let Some(sequence) = self.last_sequence {
            message = message.expected_last_sequence(sequence);
        }
        if let Some(sequence) = self.last_subject_sequence {
            message = message.expected_last_subject_sequence(sequence);
        }
        if let Some(id) = &self.last_message_id {
            message = message.expected_last_message_id(id);
        }
        message
    }
}

impl PublishPolicy<ConnectedNatsBroker> for JetStreamPublish {
    type Live = JetStreamPublisher;

    async fn pair(self, connected: &ConnectedNatsBroker) -> Result<Self::Live, PairError> {
        Ok(self.bind(connected))
    }
}

impl NatsPublishPolicy for JetStreamPublish {
    fn bind(self, connected: &ConnectedNatsBroker) -> Self::Live {
        JetStreamPublisher {
            connection: Arc::clone(connected.connection()),
            context: connected.jetstream(),
            policy: self,
        }
    }
}

/// The live `JetStream` publisher. Cheap to clone.
///
/// Every publish waits for the stream's acknowledgement, so a rejected message (unknown stream,
/// violated expectation, storage failure) is an error rather than a silent drop. Like the Core
/// publisher it aliases the connection and may outlive it: after the broker shuts down every
/// publish reports [`NatsError::Closed`].
#[derive(Clone)]
pub struct JetStreamPublisher {
    connection: Arc<NatsConnection>,
    context: Context,
    policy: JetStreamPublish,
}

impl Debug for JetStreamPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamPublisher")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl JetStreamPublisher {
    /// Publishes into the stream and returns the acknowledgement: the stream the message landed
    /// in, its sequence there, and whether the deduplication window recognised it as a duplicate.
    ///
    /// [`Publisher::publish`] is this call with the acknowledgement discarded.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Closed`] when the broker has shut down, [`NatsError::Publish`] when
    /// the message cannot be sent, and [`NatsError::JetStream`] when the stream rejects it (no
    /// such stream, or an expectation from the policy did not hold).
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: dropping the future after the message is on the wire abandons the
    /// acknowledgement, leaving the publish in an indeterminate state.
    pub async fn publish_ack(&self, msg: OutgoingMessage<'_>) -> Result<PublishAck, NatsError> {
        // Checked before the send: the context caches a client clone that would happily queue a
        // publish into a drained connection.
        self.connection.live_client(msg.name())?;

        let mut message = PublishMessage::build().payload(Bytes::copy_from_slice(msg.payload()));
        if let Some(headers) = headers_to_nats(msg.headers()) {
            message = message.headers(headers);
        }

        self.context
            .send_publish(msg.name().to_owned(), self.policy.apply(message))
            .await
            .map_err(|err| NatsError::Publish(Box::new(err)))?
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))
    }
}

impl Publisher for JetStreamPublisher {
    type Error = NatsError;

    /// # Cancel safety
    ///
    /// Not cancel-safe; see [`publish_ack`](Self::publish_ack).
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.publish_ack(msg).await.map(|_ack| ())
    }
}
