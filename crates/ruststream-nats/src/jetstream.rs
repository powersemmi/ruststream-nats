//! `JetStream` publishing: the [`JetStreamPublish`] policy, its [`JetStreamOptions`] per-message
//! settings, and the live [`JetStreamPublisher`].
//!
//! `JetStream` publishing is a different contract from Core NATS, not a mode of it: the server
//! answers every publish with an acknowledgement, and the message may state what it expects the
//! stream to look like. Both live on this pair, so the Core publisher keeps the fire-and-forget
//! shape the transport actually has.

use std::fmt::{Debug, Formatter};
use std::future::{Future, ready};
use std::sync::Arc;

use async_nats::jetstream::Context;
use async_nats::jetstream::message::PublishMessage;
use bytes::Bytes;
#[cfg(feature = "testing")]
use ruststream::HeaderMap;
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedNatsBroker, NatsConnection};
use crate::publisher::NatsPublishPolicy;
use crate::{convert::headers_to_nats, error::NatsError};

/// The acknowledgement a `JetStream` stream returns for an accepted publish.
pub use async_nats::jetstream::publish::PublishAck;

/// The `JetStream` protocol header names, as `async-nats` writes them on the wire.
#[cfg(feature = "testing")]
const MESSAGE_ID: &str = "Nats-Msg-Id";
#[cfg(feature = "testing")]
const EXPECTED_LAST_SEQUENCE: &str = "Nats-Expected-Last-Sequence";
#[cfg(feature = "testing")]
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";
#[cfg(feature = "testing")]
const EXPECTED_LAST_MESSAGE_ID: &str = "Nats-Expected-Last-Msg-Id";
#[cfg(feature = "testing")]
const EXPECTED_STREAM: &str = "Nats-Expected-Stream";

/// What one `JetStream` publish states about itself, over and above its subject and payload.
///
/// Every field is optional, and a call carries only what it set. These are protocol fields the
/// server reads, not headers of the application: a deduplication id, and the three expectations
/// that hold an optimistic-concurrency chain together.
///
/// A publish that violates an expectation is refused by the stream, and the publish returns
/// [`NatsError::JetStream`]. That is the point of stating one: the writer learns that the stream
/// moved under it instead of appending over someone else's message.
///
/// The fields are public because a test names the value the publish carried, as
/// `tb.out::<Marker>().assert_called_once().with_options(&JetStreamOptions { .. })`.
///
/// # Examples
///
/// ```
/// use ruststream_nats::JetStreamOptions;
///
/// let options = JetStreamOptions {
///     message_id: Some("order-7".into()),
///     ..JetStreamOptions::default()
/// };
/// assert_eq!(options.expect_last_sequence, None);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JetStreamOptions {
    /// The `Nats-Msg-Id` the stream's deduplication window recognises a repeat by.
    pub message_id: Option<String>,
    /// Requires the stream's last sequence to be this at the moment of the publish.
    pub expect_last_sequence: Option<u64>,
    /// Requires the last sequence *on the published subject* to be this.
    pub expect_last_subject_sequence: Option<u64>,
    /// Requires the stream's last `Nats-Msg-Id` to be this.
    pub expect_last_message_id: Option<String>,
}

impl JetStreamOptions {
    /// Writes the fields this value set as the `JetStream` protocol headers a server reads.
    ///
    /// The real publisher lets `async-nats` write them; the in-process transport has no such
    /// builder, so it writes the same headers itself and a test reads back what a server would
    /// have seen.
    #[cfg(feature = "testing")]
    pub(crate) fn write_headers(&self, headers: &mut HeaderMap) {
        if let Some(id) = &self.message_id {
            headers.insert(MESSAGE_ID, id.clone());
        }
        if let Some(sequence) = self.expect_last_sequence {
            headers.insert(EXPECTED_LAST_SEQUENCE, sequence.to_string());
        }
        if let Some(sequence) = self.expect_last_subject_sequence {
            headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, sequence.to_string());
        }
        if let Some(id) = &self.expect_last_message_id {
            headers.insert(EXPECTED_LAST_MESSAGE_ID, id.clone());
        }
    }

    /// Writes the fields this value set onto one outgoing `JetStream` message.
    pub(crate) fn apply(&self, mut message: PublishMessage) -> PublishMessage {
        if let Some(id) = &self.message_id {
            message = message.message_id(id);
        }
        if let Some(sequence) = self.expect_last_sequence {
            message = message.expected_last_sequence(sequence);
        }
        if let Some(sequence) = self.expect_last_subject_sequence {
            message = message.expected_last_subject_sequence(sequence);
        }
        if let Some(id) = &self.expect_last_message_id {
            message = message.expected_last_message_id(id);
        }
        message
    }
}

/// The per-message `JetStream` settings, as steps on the publish builder.
///
/// A handler body that adjusts one of these names this crate's prelude and bounds its slot
/// `Out<impl Publisher<Options = JetStreamOptions>, Marker>`. That body is tied to `JetStream`,
/// and its signature says so; a body that publishes plainly stays on the framework prelude alone.
///
/// The steps sit on the builder rather than on a publisher of their own, so the publish they
/// finish still travels through the mount site's entry, with the codec and the transforms that
/// entry named.
///
/// # Examples
///
/// ```no_run
/// use ruststream_nats::prelude::*;
/// # use serde::Serialize;
/// #[derive(Serialize, Outgoing)]
/// struct Archived {
///     id: u64,
/// }
///
/// # async fn record(
/// #     archived: &Archived,
/// #     ledger: impl Publisher<Options = JetStreamOptions>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// ledger
///     .message(archived)
///     .to("orders.archived")
///     .message_id(format!("order-{}", archived.id))
///     .expect_last_subject_sequence(41)
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait JetStreamPublishSteps {
    /// Tags this message with a deduplication id, so a repeat inside the stream's deduplication
    /// window is stored once.
    #[must_use]
    fn message_id(self, id: impl Into<String>) -> Self;

    /// Requires the stream's last sequence to be `sequence`, or the stream refuses this message.
    #[must_use]
    fn expect_last_sequence(self, sequence: u64) -> Self;

    /// Requires the last sequence on this message's own subject to be `sequence`.
    #[must_use]
    fn expect_last_subject_sequence(self, sequence: u64) -> Self;

    /// Requires the stream's last `Nats-Msg-Id` to be `id`.
    #[must_use]
    fn expect_last_message_id(self, id: impl Into<String>) -> Self;
}

/// The bound on the sink's options type is what keeps these steps off a builder over any other
/// publisher, this crate's Core NATS one included.
impl<Sink, Body, Enc, Hdrs, Dest> JetStreamPublishSteps
    for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = JetStreamOptions>,
{
    fn message_id(mut self, id: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(JetStreamOptions::default)
            .message_id = Some(id.into());
        self
    }

    fn expect_last_sequence(mut self, sequence: u64) -> Self {
        self.options_mut()
            .get_or_insert_with(JetStreamOptions::default)
            .expect_last_sequence = Some(sequence);
        self
    }

    fn expect_last_subject_sequence(mut self, sequence: u64) -> Self {
        self.options_mut()
            .get_or_insert_with(JetStreamOptions::default)
            .expect_last_subject_sequence = Some(sequence);
        self
    }

    fn expect_last_message_id(mut self, id: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(JetStreamOptions::default)
            .expect_last_message_id = Some(id.into());
        self
    }
}

/// The `JetStream` publish policy: pure declaration, constructible anywhere.
///
/// It carries what belongs to the publisher rather than to a message, which on `JetStream` is one
/// thing: the stream the subject must be served by. A publisher that states
/// `expect_stream("ORDERS")` fails loudly if its subject is routed elsewhere, instead of writing
/// somewhere unintended.
///
/// The sequence and message-id expectations are not here. They belong to one message - an expected
/// last sequence fixed for the life of a publisher can hold for at most one publish - so they are
/// [`JetStreamOptions`] fields, set by the [`JetStreamPublishSteps`] steps at the call site.
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
    stream: Option<String>,
}

impl JetStreamPublish {
    /// Requires the subject to be served by the named stream. The server rejects the publish
    /// otherwise, so a misrouted subject surfaces as an error rather than a silent write.
    pub fn expect_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Applies the publisher's own declaration to one outgoing `JetStream` message.
    fn apply(&self, message: PublishMessage) -> PublishMessage {
        match &self.stream {
            Some(stream) => message.expected_stream(stream),
            None => message,
        }
    }

    /// The same declaration as the protocol header a server reads. See
    /// [`JetStreamOptions::write_headers`].
    #[cfg(feature = "testing")]
    pub(crate) fn write_headers(&self, headers: &mut HeaderMap) {
        if let Some(stream) = &self.stream {
            headers.insert(EXPECTED_STREAM, stream.clone());
        }
    }
}

impl PublishPolicy<ConnectedNatsBroker> for JetStreamPublish {
    type Live = JetStreamPublisher;

    fn pair(
        self,
        connected: &ConnectedNatsBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
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
    /// [`Publisher::publish`] is this call with the acknowledgement discarded. `options` is what
    /// this one message states about itself; pass `None` to publish on the policy alone.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Closed`] when the broker has shut down, [`NatsError::Publish`] when
    /// the message cannot be sent, and [`NatsError::JetStream`] when the stream rejects it (no
    /// such stream, or an expectation did not hold).
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe: dropping the future after the message is on the wire abandons the
    /// acknowledgement, leaving the publish in an indeterminate state.
    pub async fn publish_ack(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&JetStreamOptions>,
    ) -> Result<PublishAck, NatsError> {
        // Checked before the send: the context caches a client clone that would happily queue a
        // publish into a drained connection.
        self.connection.live_client(msg.name())?;

        let mut message = PublishMessage::build().payload(Bytes::copy_from_slice(msg.payload()));
        // The application's headers go on first: `headers` replaces the map, and the protocol
        // fields below are written into it.
        if let Some(headers) = headers_to_nats(msg.headers()) {
            message = message.headers(headers);
        }
        message = self.policy.apply(message);
        if let Some(options) = options {
            message = options.apply(message);
        }

        self.context
            .send_publish(msg.name().to_owned(), message)
            .await
            .map_err(|err| NatsError::Publish(Box::new(err)))?
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))
    }
}

impl Publisher for JetStreamPublisher {
    type Error = NatsError;
    type Options = JetStreamOptions;

    /// # Cancel safety
    ///
    /// Not cancel-safe; see [`publish_ack`](Self::publish_ack).
    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        self.publish_ack(msg, options).await.map(|_ack| ())
    }
}
