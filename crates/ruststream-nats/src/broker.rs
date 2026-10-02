//! The broker ladder: [`NatsBroker`] -> [`ConnectedNatsBroker`] -> [`ClosedNatsBroker`].
//!
//! Construction is synchronous and I/O-free; the connection is dialled by the consuming
//! [`Broker::connect`], and the connected form is the only value carrying a publish or subscribe
//! surface. [`ConnectedBroker::shutdown`] consumes it in turn and returns the terminal witness.

// Without the `testing` feature a connection link has one variant, so a `match` on it has a
// single arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::fmt::{Debug, Formatter};
use std::future::Future;
#[cfg(feature = "testing")]
use std::future::ready;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::{PullConsumer, pull::Config as ConsumerConfig};
use async_nats::{Client, ConnectOptions};
#[cfg(feature = "testing")]
use bytes::Bytes;
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe,
};
#[cfg(feature = "testing")]
use ruststream::{OutgoingMessage, RawMessage};
use tokio::runtime::Handle;

#[cfg(feature = "testing")]
use crate::in_process::{self, Bus, Origin, PublishMode, RouteBook};
use crate::{
    error::NatsError,
    publisher::{NatsPublish, NatsPublishPolicy},
    subject::{CoreSubject, NatsSubscription, SubscriptionPlan},
    subscriber::NatsSubscriber,
};

/// The live connection, shared by the connected broker and every publisher paired off it.
pub(crate) struct NatsConnection {
    client: Client,
    closed: AtomicBool,
    /// The runtime the broker connected on, where the tasks the client starts on the broker's
    /// behalf (a consumer's pull loop, a `JetStream` context's acknowledgement task) run whichever
    /// thread asks for them.
    ///
    /// Why optional: a client adopted through `from_client` may be handed over outside any
    /// runtime, and then the caller's runtime is the only one there is.
    runtime: Option<Handle>,
}

impl Debug for NatsConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsConnection")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl NatsConnection {
    fn new(client: Client, runtime: Option<Handle>) -> Arc<Self> {
        Arc::new(Self {
            client,
            closed: AtomicBool::new(false),
            runtime,
        })
    }

    /// A `JetStream` context on this connection. The context starts its acknowledgement task as
    /// it is built, so it is built inside the runtime the broker connected on.
    pub(crate) fn jetstream(&self) -> jetstream::Context {
        let _entered = self.runtime.as_ref().map(Handle::enter);
        jetstream::new(self.client.clone())
    }

    /// Runs `work` on the runtime the broker connected on, so a task the client spawns inside it
    /// lands there and not on the caller's runtime.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::JetStream`] when that runtime is shutting down and drops the work.
    async fn on_runtime<Output: Send + 'static>(
        &self,
        work: impl Future<Output = Output> + Send + 'static,
    ) -> Result<Output, NatsError> {
        let Some(runtime) = &self.runtime else {
            return Ok(work.await);
        };
        runtime.spawn(work).await.map_err(|err| {
            if err.is_panic() {
                resume_unwind(err.into_panic());
            }
            NatsError::JetStream(Box::new(err))
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

/// What a connected broker and every handle paired off it speak over: the live connection, or,
/// under the `testing` feature, the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the connection handle itself and
/// every `match` on it is irrefutable: a production build carries no second transport and no
/// branch to it.
#[derive(Debug, Clone)]
pub(crate) enum Link {
    Nats(Arc<NatsConnection>),
    #[cfg(feature = "testing")]
    InProcess(Arc<Bus>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the connection handle it wraps, and the connected broker nothing more.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Link>() == size_of::<Arc<NatsConnection>>());
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<ConnectedNatsBroker>() == size_of::<Arc<NatsConnection>>());

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
        #[cfg(feature = "testing")]
        let routes = RouteBook::new(in_process::settings(&self.options)?.no_echo);
        let client = self
            .options
            .connect(self.addrs.as_str())
            .await
            .map_err(|err| NatsError::Connect(Box::new(err)))?;
        Ok(ConnectedNatsBroker {
            link: Link::Nats(NatsConnection::new(client, Some(Handle::current()))),
            #[cfg(feature = "testing")]
            routes,
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, carrying the
/// in-process transport in place of the connection.
///
/// The addresses are parsed as `connect` parses them, so a broker a service could not connect is
/// not one a test can connect either, and the connection options that decide what the server
/// delivers (`no_echo`, the inbox prefix) are the ones [`with_options`](NatsBroker::with_options)
/// set.
#[cfg(feature = "testing")]
impl InProcess for NatsBroker {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        ready(
            in_process::connect(&self.addrs, &self.options).map(|bus| ConnectedNatsBroker {
                routes: RouteBook::new(bus.no_echo()),
                link: Link::InProcess(bus),
            }),
        )
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(NatsBroker);

/// `DescribeServer` reports the host and port of every configured address, which is what the
/// `AsyncAPI` document records for the service. The live coordinates the server reports once
/// connected are on [`ConnectedNatsBroker`].
///
/// Credentials are not part of a coordinate. `addrs` goes to the client as written, and the client
/// accepts `nats://user:password@host` and `nats://token@host`, but the generated document is
/// published and shared, so what a URL carries to authenticate the connection stops here. The
/// framework's [`ServerSpec::host_from_url`] does the cutting, once per configured address.
impl DescribeServer for NatsBroker {
    fn describe_server(&self) -> ServerSpec {
        let hosts = self
            .addrs
            .split(',')
            .map(|addr| ServerSpec::host_from_url(addr.trim()))
            .filter(|host| !host.is_empty())
            .collect::<Vec<_>>()
            .join(",");
        ServerSpec::new(hosts, "nats")
    }
}

/// The typed witness that [`Broker::connect`] succeeded: holds the live connection.
///
/// Everything connection-bound hangs off this value: subscriptions ([`Subscribe`],
/// [`CoreSubject`]) and publishers ([`publisher`](Self::publisher)).
/// [`ConnectedBroker::shutdown`] consumes it, so a publish or subscribe after shutdown is a
/// compile error for the owner of the handle.
#[derive(Debug)]
pub struct ConnectedNatsBroker {
    link: Link,
    /// The subscriptions this broker opened, which the test harness asks its routing of.
    #[cfg(feature = "testing")]
    routes: RouteBook,
}

impl ConnectedNatsBroker {
    /// Adopts an already-connected `async-nats` client as the connected form.
    ///
    /// The escape hatch for a client built outside the framework (a shared client, or an
    /// authentication flow `ConnectOptions` cannot express). Prefer
    /// [`NatsBroker::with_options`] where it fits: only the plain [`NatsBroker`] slots into the
    /// synchronous app builder.
    ///
    /// The broker's own tasks run on the runtime this is called on, when there is one.
    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            link: Link::Nats(NatsConnection::new(client, Handle::try_current().ok())),
            #[cfg(feature = "testing")]
            routes: RouteBook::new(false),
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
    ///
    /// # Panics
    ///
    /// Panics on a broker the test harness connected in process (the `testing` feature's
    /// `connect_in_process`): that broker has no client, and what a test does through one is
    /// outside the in-process transport.
    #[must_use]
    pub fn client(&self) -> Client {
        self.connection().client().clone()
    }

    /// The live connection.
    ///
    /// Why this can panic: under the `testing` feature the same connected type carries the
    /// in-process transport, which has no client to hand out; without the feature the match is
    /// irrefutable.
    fn connection(&self) -> &Arc<NatsConnection> {
        match &self.link {
            Link::Nats(connection) => connection,
            #[cfg(feature = "testing")]
            Link::InProcess(_) => panic!(
                "a broker connected in process has no async-nats client: `client` and \
                 `jetstream` are for a broker connected with `connect`"
            ),
        }
    }

    /// The coordinates the server announced on this connection, which may differ from the
    /// configured address (a cluster route, a discovered peer), together with the version of the
    /// NATS client protocol it speaks.
    ///
    /// The protocol version is a fact of the live connection, so it is here rather than on
    /// [`NatsBroker::describe_server`], which answers before anything is dialled and is what the
    /// generated `AsyncAPI` document takes its server from.
    #[must_use]
    pub fn server_spec(&self) -> ServerSpec {
        let connection = match &self.link {
            Link::Nats(connection) => connection,
            #[cfg(feature = "testing")]
            Link::InProcess(_) => return ServerSpec::in_process("nats"),
        };
        let info = connection.client().server_info();
        ServerSpec::new(format!("{}:{}", info.host, info.port), "nats")
            .protocol_version(info.proto.to_string())
    }

    /// A `JetStream` context on this connection, for stream and consumer administration
    /// (creating the stream a consumer reads, purging it, deleting it on teardown).
    ///
    /// # Panics
    ///
    /// Panics on a broker connected in process, as [`client`](Self::client) does.
    #[must_use]
    pub fn jetstream(&self) -> jetstream::Context {
        self.connection().jetstream()
    }

    /// What every handle paired off this broker speaks over.
    pub(crate) const fn link(&self) -> &Link {
        &self.link
    }

    /// Opens the subscription `source` describes: a Core subscription for [`CoreSubject`], a pull
    /// consumer for [`JetStreamSubject`](crate::JetStreamSubject).
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when the subject is empty, [`NatsError::Subscribe`]
    /// when the broker rejects a Core subscription, or [`NatsError::JetStream`] when the
    /// `JetStream` stream or consumer cannot be resolved.
    pub async fn subscribe_with<S: NatsSubscription>(
        &self,
        source: S,
    ) -> Result<NatsSubscriber, NatsError> {
        source.ensure_subject()?;
        #[cfg(feature = "testing")]
        if let Link::InProcess(bus) = &self.link {
            return in_process::subscribe(bus, &self.routes, &source);
        }
        #[cfg(feature = "testing")]
        let route = in_process::route(&source);
        let subscriber = self.subscribe_live(source).await?;
        #[cfg(feature = "testing")]
        if let Some(route) = route {
            self.routes.record(route);
        }
        Ok(subscriber)
    }

    async fn subscribe_live<S: NatsSubscription>(
        &self,
        source: S,
    ) -> Result<NatsSubscriber, NatsError> {
        let subject = source.subject();
        match source.plan() {
            SubscriptionPlan::Core { queue_group } => {
                self.subscribe_core(subject, queue_group).await
            }
            SubscriptionPlan::JetStream {
                stream,
                durable,
                filter_subject,
                ack_wait,
                max_ack_pending,
                deliver_policy,
                pull_expires,
            } => {
                let consumer_cfg = ConsumerConfig {
                    durable_name: durable.map(str::to_owned),
                    filter_subject: filter_subject.to_owned(),
                    max_ack_pending,
                    ack_wait,
                    deliver_policy,
                    ..Default::default()
                };
                self.subscribe_jetstream(subject, stream, consumer_cfg, pull_expires)
                    .await
            }
        }
    }

    async fn subscribe_core(
        &self,
        subject: &str,
        queue_group: Option<&str>,
    ) -> Result<NatsSubscriber, NatsError> {
        let client = self.connection().live_client(subject)?;
        let subject = subject.to_owned();
        let inner = if let Some(queue) = queue_group {
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
        subject: &str,
        stream_name: &str,
        consumer_cfg: ConsumerConfig,
        pull_expires: Duration,
    ) -> Result<NatsSubscriber, NatsError> {
        let connection = self.connection();
        connection.live_client(subject)?;
        let ctx = connection.jetstream();
        let stream = ctx
            .get_stream(stream_name)
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))?;

        let consumer: PullConsumer = stream
            .create_consumer(consumer_cfg)
            .await
            .map_err(|err| NatsError::JetStream(Box::new(err)))?;
        // The stream spawns the consumer's pull loop as it opens.
        let pulling = consumer.clone();
        let messages = connection
            .on_runtime(async move { pulling.messages().await })
            .await?
            .map_err(|err| NatsError::JetStream(Box::new(err)))?;

        Ok(NatsSubscriber::from_jetstream(
            subject.to_owned(),
            stream_name.to_owned(),
            messages,
            consumer,
            pull_expires,
        ))
    }
}

impl ConnectedBroker for ConnectedNatsBroker {
    type Error = NatsError;
    type Closed = ClosedNatsBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        let connection = match self.link {
            Link::Nats(connection) => connection,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => {
                let (messages_sent, messages_received) = bus.close();
                return Ok(ClosedNatsBroker {
                    messages_sent,
                    messages_received,
                    connects: 1,
                });
            }
        };
        // Marked closed before draining: a publisher aliasing the connection must not slip a
        // message into a connection that is already going away.
        connection.closed.store(true, Ordering::Release);
        let client = connection.client();
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

    /// A NATS subject is subscribed to and published to under one name, so a bare
    /// `#[subscriber("orders.created")]` is a destination a copy of a delivery reaches: the
    /// framework publishes a deferred retry back to the name itself and the mount site owes
    /// nothing.
    ///
    /// A pattern is the exception, and it is not reachable from here: the by-name form takes one
    /// subject, and `CoreWildcard::new("orders.*")` is the descriptor that reads many.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(CoreSubject::new(name)).await
    }
}

impl DefaultPublish for ConnectedNatsBroker {
    type Policy = NatsPublish;
}

/// The harness's view of the broker: the in-process transport it injects into and reads back,
/// the coordinator it counts in-flight deliveries with, and the routing of every subscription
/// this broker opened, in process or live (see [`routes`](TestableBroker::routes)).
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the connection `connect_in_process` produced, and a live connection has no log to read and no
/// synchronous way to take a message.
#[cfg(feature = "testing")]
impl TestableBroker for ConnectedNatsBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Link::InProcess(bus) = &self.link {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let bus = self.bus("inject");
        // An external producer: another connection, so `no_echo` does not hold it back.
        if let Err(err) = bus.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers(),
            None,
            PublishMode::Core,
            Origin::External,
        ) {
            panic!(
                "the injected message to {:?} is not one the server takes: {err}",
                message.name()
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published").published(name)
    }

    /// NATS routing, answered from the subscriptions this broker opened: every Core subscription
    /// whose subject matches, one member of a queue group, and the consumers of the stream that
    /// stores the subject under their filter. See the crate's testing overview for the rules.
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        self.routes.routes(destination, subscriptions)
    }
}

#[cfg(feature = "testing")]
impl ConnectedNatsBroker {
    /// The in-process transport, which is all the harness drives.
    fn bus(&self, what: &str) -> &Arc<Bus> {
        match &self.link {
            Link::InProcess(bus) => bus,
            Link::Nats(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the connection `connect_in_process` produces"
            ),
        }
    }
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

    #[test]
    fn the_host_survives_every_address_shape_the_client_accepts() {
        for (addr, expected) in [
            ("nats://127.0.0.1:4222", "127.0.0.1:4222"),
            ("tls://nats.example.com:4222", "nats.example.com:4222"),
            // A bare address, which the client also takes.
            ("nats.example.com:4222", "nats.example.com:4222"),
            ("nats://nats.example.com", "nats.example.com"),
            // User and password.
            (
                "nats://alice:s3cret@nats.example.com:4222",
                "nats.example.com:4222",
            ),
            // A token: no colon and no user name, so a parse looking for `user:pass` misses it.
            (
                "nats://s3cret-token@nats.example.com:4222",
                "nats.example.com:4222",
            ),
            // A password may contain the separator, so inside the authority the last `@` wins.
            (
                "nats://alice:p@ss@nats.example.com:4222",
                "nats.example.com:4222",
            ),
            // The authority ends before the path or the query, so an `@` past it separates
            // nothing. Cutting on `@` first would report a host of `b`.
            ("nats://nats.example.com:4222/a@b", "nats.example.com:4222"),
            (
                "nats://nats.example.com:4222/?token=a@b",
                "nats.example.com:4222",
            ),
            ("nats://nats.example.com:4222#a@b", "nats.example.com:4222"),
        ] {
            let spec = NatsBroker::new(addr).describe_server();
            assert_eq!(spec.host.as_deref(), Some(expected), "parsing {addr}");
        }
    }

    #[test]
    fn a_list_of_addresses_describes_every_host_and_no_credentials() {
        let spec = NatsBroker::new(
            "nats://alice:s3cret@one.example.com:4222, nats://tok@two.example.com:4223",
        )
        .describe_server();
        let host = spec.host.expect("a networked broker states its host");

        assert_eq!(host, "one.example.com:4222,two.example.com:4223");
        assert!(
            !host.contains('@'),
            "the userinfo separator is gone: {host}"
        );
    }

    /// The document a service generates is meant to be published, so an address that
    /// authenticates the connection must not describe the server it reaches.
    #[test]
    fn an_address_carrying_credentials_describes_a_server_without_them() {
        for addr in [
            "nats://alice:s3cret@nats.example.com:4222",
            "nats://s3cret@nats.example.com:4222",
        ] {
            let host = NatsBroker::new(addr)
                .describe_server()
                .host
                .expect("a networked broker states its host");

            assert_eq!(host, "nats.example.com:4222");
            assert!(!host.contains("s3cret"), "{addr} leaked {host:?}");
            assert!(!host.contains("alice"), "{addr} leaked {host:?}");
            assert!(!host.contains('@'), "{addr} leaked {host:?}");
        }
    }
}
