//! The broker ladder: [`NatsBroker`] -> [`ConnectedNatsBroker`] -> [`ClosedNatsBroker`].
//!
//! Construction is synchronous and I/O-free; the connection is dialled by the consuming
//! [`Broker::connect`], and the connected form is the only value carrying a publish or subscribe
//! surface. [`ConnectedBroker::shutdown`] consumes it in turn and returns the terminal witness.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_nats::jetstream;
use async_nats::jetstream::consumer::{PullConsumer, pull::Config as ConsumerConfig};
use async_nats::{Client, ConnectOptions};
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};

use crate::{
    error::NatsError,
    publisher::{NatsPublish, NatsPublishPolicy},
    subscribe_options::SubscribeOptions,
    subscriber::NatsSubscriber,
};

/// The live connection, shared by the connected broker and every publisher paired off it.
pub(crate) struct NatsConnection {
    client: Client,
    closed: AtomicBool,
}

impl Debug for NatsConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsConnection")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl NatsConnection {
    fn new(client: Client) -> Arc<Self> {
        Arc::new(Self {
            client,
            closed: AtomicBool::new(false),
        })
    }

    /// The client, or [`NatsError::Closed`] once the broker has shut down.
    ///
    /// Why this stays a runtime check: publishers paired before the shutdown alias the connection
    /// and outlive it, and the typed ladder can only rule out misuse through the owner's handle.
    pub(crate) fn live_client(&self, subject: &str) -> Result<&Client, NatsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NatsError::Closed {
                subject: subject.to_owned(),
            });
        }
        Ok(&self.client)
    }

    pub(crate) const fn client(&self) -> &Client {
        &self.client
    }
}

/// A NATS broker: configuration captured, no I/O performed yet.
///
/// [`new`](Self::new) is synchronous and records only the server address, so a NATS service is
/// assembled with the synchronous `#[ruststream::app]` builder like any other broker. The runtime
/// calls [`Broker::connect`] once at startup, which consumes this value and yields the
/// [`ConnectedNatsBroker`] witness: subscriptions and publishers exist only from there, so "not
/// connected" is not representable.
///
/// Authentication, TLS, and other client tuning ride an
/// [`async_nats::ConnectOptions`](ConnectOptions) attached with [`with_options`](Self::with_options);
/// building the options performs no I/O either.
///
/// # Examples
///
/// ```
/// use ruststream_nats::NatsBroker;
///
/// let broker = NatsBroker::new("nats://localhost:4222");
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct NatsBroker {
    addrs: String,
    options: ConnectOptions,
}

impl NatsBroker {
    /// Records the server address (`nats://host:port`, or a comma-separated list). No I/O.
    pub fn new(addrs: impl Into<String>) -> Self {
        Self {
            addrs: addrs.into(),
            options: ConnectOptions::default(),
        }
    }

    /// Sets the `async-nats` connection options used when [`Broker::connect`] dials the server:
    /// credentials, TLS, ping interval, reconnect behaviour.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_nats::ConnectOptions;
    /// use ruststream_nats::NatsBroker;
    ///
    /// let broker = NatsBroker::new("nats://localhost:4222")
    ///     .with_options(ConnectOptions::with_user_and_password("svc".into(), "secret".into()));
    /// # let _ = broker;
    /// ```
    pub fn with_options(mut self, options: ConnectOptions) -> Self {
        self.options = options;
        self
    }

    /// The configured server address.
    #[must_use]
    pub fn addrs(&self) -> &str {
        &self.addrs
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;
    type Connected = ConnectedNatsBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let client = self
            .options
            .connect(self.addrs.as_str())
            .await
            .map_err(|err| NatsError::Connect(Box::new(err)))?;
        Ok(ConnectedNatsBroker::from_client(client))
    }
}

/// `DescribeServer` reports the configured NATS address, which is what the `AsyncAPI` document
/// records for the service. The live coordinates the server reports once connected are on
/// [`ConnectedNatsBroker`].
impl DescribeServer for NatsBroker {
    fn describe_server(&self) -> ServerSpec {
        let host = self
            .addrs
            .trim_start_matches("nats://")
            .trim_start_matches("tls://")
            .to_owned();
        ServerSpec::new(host, "nats")
    }
}

/// The typed witness that [`Broker::connect`] succeeded: holds the live connection.
///
/// Everything connection-bound hangs off this value: subscriptions ([`Subscribe`],
/// [`SubscribeOptions`]) and publishers ([`publisher`](Self::publisher)).
/// [`ConnectedBroker::shutdown`] consumes it, so a publish or subscribe after shutdown is a
/// compile error for the owner of the handle.
#[derive(Debug)]
pub struct ConnectedNatsBroker {
    connection: Arc<NatsConnection>,
}

impl ConnectedNatsBroker {
    /// Adopts an already-connected `async-nats` client as the connected form.
    ///
    /// The escape hatch for a client built outside the framework (a shared client, or an
    /// authentication flow `ConnectOptions` cannot express). Prefer
    /// [`NatsBroker::with_options`] where it fits: only the plain [`NatsBroker`] slots into the
    /// synchronous app builder.
    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            connection: NatsConnection::new(client),
        }
    }

    /// A live publisher for `policy`.
    ///
    /// [`NatsPublish`] pairs into the Core NATS publisher (request/reply included);
    /// [`JetStreamPublish`](crate::JetStreamPublish) pairs into the `JetStream` publisher, which
    /// awaits the stream's publish acknowledgement. Both are cheap to build and cheap to clone.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::Broker;
    /// use ruststream_nats::{JetStreamPublish, NatsBroker, NatsPublish};
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = NatsBroker::new("nats://localhost:4222").connect().await?;
    /// let core = connected.publisher(NatsPublish);
    /// let jetstream = connected.publisher(JetStreamPublish::default().expect_stream("ORDERS"));
    /// # let _ = (core, jetstream);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn publisher<P: NatsPublishPolicy>(&self, policy: P) -> P::Live {
        policy.bind(self)
    }

    /// A clone of the underlying `async-nats` client, for operations this crate does not wrap.
    #[must_use]
    pub fn client(&self) -> Client {
        self.connection.client().clone()
    }

    /// The coordinates the server announced on this connection, which may differ from the
    /// configured address (a cluster route, a discovered peer).
    #[must_use]
    pub fn server_spec(&self) -> ServerSpec {
        let info = self.connection.client().server_info();
        ServerSpec::new(format!("{}:{}", info.host, info.port), "nats")
    }

    /// A `JetStream` context on this connection, for stream and consumer administration
    /// (creating the stream a consumer reads, purging it, deleting it on teardown).
    #[must_use]
    pub fn jetstream(&self) -> jetstream::Context {
        jetstream::new(self.client())
    }

    pub(crate) fn connection(&self) -> &Arc<NatsConnection> {
        &self.connection
    }

    /// Opens a subscription described by `opts`. Selects Core or `JetStream` based on whether
    /// [`SubscribeOptions::jetstream`] was called.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when `opts` mixes Core and `JetStream` fields
    /// incompatibly, [`NatsError::Subscribe`] when the broker rejects a Core subscription, or
    /// [`NatsError::JetStream`] when the `JetStream` stream or consumer cannot be resolved.
    pub async fn subscribe_with(
        &self,
        opts: SubscribeOptions,
    ) -> Result<NatsSubscriber, NatsError> {
        opts.validate()?;
        if opts.is_jetstream() {
            self.subscribe_jetstream(opts).await
        } else {
            self.subscribe_core(opts).await
        }
    }

    async fn subscribe_core(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let client = self.connection.live_client(opts.subject())?;
        let subject = opts.subject().to_owned();
        let inner = if let Some(queue) = opts.queue_group_ref() {
            client
                .queue_subscribe(subject.clone(), queue.to_owned())
                .await
                .map_err(|err| NatsError::Subscribe(Box::new(err)))?
        } else {
            client
                .subscribe(subject.clone())
                .await
                .map_err(|err| NatsError::Subscribe(Box::new(err)))?
        };
        // Core `SUB` is written without waiting for the server, so without this round trip a
        // producer on another connection can publish into a subscription the server has not
        // registered yet and the message is simply lost. Startup pays one flush per subscription;
        // the JetStream path needs none, its consumer creation is already a request/reply.
        client
            .flush()
            .await
            .map_err(|err| NatsError::Subscribe(Box::new(err)))?;
        Ok(NatsSubscriber::from_core(subject, inner))
    }

    async fn subscribe_jetstream(
        &self,
        opts: SubscribeOptions,
    ) -> Result<NatsSubscriber, NatsError> {
        let client = self.connection.live_client(opts.subject())?.clone();
        let ctx = jetstream::new(client);
        let stream_name = opts
            .stream_ref()
            .expect("validated jetstream option")
            .to_owned();
        let stream = ctx
            .get_stream(&stream_name)
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))?;

        let consumer_cfg = ConsumerConfig {
            durable_name: opts.durable_ref().map(str::to_owned),
            filter_subject: opts.filter_subject_or_default(),
            max_ack_pending: opts.max_ack_pending_or_default(),
            ack_wait: opts.ack_wait_or_default(),
            deliver_policy: opts.deliver_policy_or_default(),
            ..Default::default()
        };
        let consumer: PullConsumer = stream
            .create_consumer(consumer_cfg)
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))?;

        Ok(NatsSubscriber::from_jetstream(
            opts.subject().to_owned(),
            stream_name,
            messages,
            consumer,
            opts.pull_batch_or_default(),
            opts.pull_expires_or_default(),
        ))
    }
}

impl ConnectedBroker for ConnectedNatsBroker {
    type Error = NatsError;
    type Closed = ClosedNatsBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        // Marked closed before draining: a publisher aliasing the connection must not slip a
        // message into a connection that is already going away.
        self.connection.closed.store(true, Ordering::Release);
        let client = self.connection.client();
        let stats = client.statistics();
        client
            .drain()
            .await
            .map_err(|err| NatsError::Shutdown(Box::new(err)))?;
        Ok(ClosedNatsBroker {
            messages_sent: stats.out_messages.load(Ordering::Relaxed),
            messages_received: stats.in_messages.load(Ordering::Relaxed),
            connects: stats.connects.load(Ordering::Relaxed),
        })
    }
}

// By-subject subscription capability: the runtime's default `Name` source resolves through this
// for the common Core-subject case.
impl Subscribe for ConnectedNatsBroker {
    type Subscriber = NatsSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(SubscribeOptions::new(name)).await
    }
}

impl DefaultPublish for ConnectedNatsBroker {
    type Policy = NatsPublish;
}

/// The terminal witness returned by shutting down a [`ConnectedNatsBroker`].
///
/// It has no publish or subscribe surface; it carries the drained connection's counters as plain
/// data, for a shutdown log line or a teardown assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedNatsBroker {
    messages_sent: u64,
    messages_received: u64,
    connects: u64,
}

impl ClosedNatsBroker {
    /// How many messages the connection published over its lifetime.
    #[must_use]
    pub const fn messages_sent(&self) -> u64 {
        self.messages_sent
    }

    /// How many messages the connection received over its lifetime.
    #[must_use]
    pub const fn messages_received(&self) -> u64 {
        self.messages_received
    }

    /// How many times the connection was established, counting reconnects.
    #[must_use]
    pub const fn connects(&self) -> u64 {
        self.connects
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `new` records the address without connecting: no server is needed to build the broker or to
    // describe it, which is what lets it slot into the synchronous app builder.
    #[test]
    fn new_performs_no_io_and_describes_the_configured_address() {
        let broker = NatsBroker::new("nats://127.0.0.1:4222");
        let spec = broker.describe_server();
        assert_eq!(spec.protocol, "nats");
        assert_eq!(spec.host.as_deref(), Some("127.0.0.1:4222"));
    }
}
