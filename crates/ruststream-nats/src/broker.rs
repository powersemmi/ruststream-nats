//! The [`NatsBroker`]: the entry point of the `async-nats` integration.

use std::sync::Arc;

use async_nats::jetstream::consumer::{PullConsumer, pull::Config as ConsumerConfig};
use ruststream::{Broker, Subscribe};
use tokio::sync::OnceCell;

use crate::{
    error::NatsError, publisher::NatsPublisher, subscribe_options::SubscribeOptions,
    subscriber::NatsSubscriber,
};

/// A NATS broker handle backed by an [`async_nats::Client`].
///
/// Construct it synchronously with [`NatsBroker::new`] and let the runtime connect it at startup,
/// or eagerly with [`NatsBroker::connect`] / [`NatsBroker::from_client`]. The handle is cheap to
/// clone, and clones share one connection. Subscriptions (Core or `JetStream`) are opened uniformly
/// through [`NatsBroker::subscribe`] with [`SubscribeOptions`]; the broker dispatches internally on
/// whether the options describe a `JetStream` consumer.
///
/// # Lazy connection
///
/// [`new`](Self::new) performs no I/O: it only records the server address. The connection is opened
/// by [`Broker::connect`], which the runtime calls once at startup, so a NATS service can be built
/// with the synchronous `#[ruststream::app]` macro. Publishers handed out before `connect` resolve
/// the shared connection on first use; operations that need it before `connect` return
/// [`NatsError::NotConnected`].
///
/// # Examples
///
/// ```no_run
/// use ruststream_nats::{NatsBroker, SubscribeOptions};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = NatsBroker::connect("nats://localhost:4222").await?;
/// let publisher = broker.publisher();
/// let core_sub = broker.subscribe(SubscribeOptions::new("orders.created")).await?;
/// let js_sub = broker
///     .subscribe(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))
///     .await?;
/// # let _ = (publisher, core_sub, js_sub);
/// broker.shutdown_client().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct NatsBroker {
    client: Arc<OnceCell<async_nats::Client>>,
    addrs: Option<String>,
}

impl std::fmt::Debug for NatsBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsBroker").finish_non_exhaustive()
    }
}

impl NatsBroker {
    /// Creates a broker that connects to `addrs` when [`Broker::connect`] runs.
    ///
    /// Synchronous and performs no I/O, so it slots into the `#[ruststream::app]` builder; the
    /// connection is opened lazily at startup. See the [type docs](Self#lazy-connection).
    #[must_use]
    pub fn new(addrs: impl Into<String>) -> Self {
        Self {
            client: Arc::new(OnceCell::new()),
            addrs: Some(addrs.into()),
        }
    }

    /// Connects to a NATS server eagerly, returning an already-connected broker.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Connect`] when the connection cannot be established.
    pub async fn connect(addrs: impl async_nats::ToServerAddrs) -> Result<Self, NatsError> {
        let client = async_nats::connect(addrs)
            .await
            .map_err(|err| NatsError::Connect(Box::new(err)))?;
        Ok(Self::from_client(client))
    }

    /// Wraps an already-connected `async-nats` client. Useful for advanced configuration
    /// (TLS, credentials, custom options).
    #[must_use]
    pub fn from_client(client: async_nats::Client) -> Self {
        Self {
            client: Arc::new(OnceCell::new_with(Some(client))),
            addrs: None,
        }
    }

    /// Returns a clone of the underlying client. Useful for advanced operations not yet covered
    /// by the wrapper, including building a `JetStream` context directly.
    ///
    /// # Panics
    ///
    /// Panics if the broker has not connected yet (built with [`new`](Self::new) and
    /// [`Broker::connect`] not run). Call it after startup, or build with [`connect`](Self::connect)
    /// / [`from_client`](Self::from_client).
    #[must_use]
    pub fn client(&self) -> async_nats::Client {
        self.client
            .get()
            .cloned()
            .expect("NatsBroker::client() called before connect()")
    }

    /// The connected client, or [`NatsError::NotConnected`] when `connect` has not run yet.
    fn connected(&self) -> Result<async_nats::Client, NatsError> {
        self.client.get().cloned().ok_or(NatsError::NotConnected)
    }

    /// Opens a subscription described by `opts`. Selects Core or `JetStream` based on whether
    /// [`SubscribeOptions::jetstream`] was called.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::NotConnected`] when the broker has not connected,
    /// [`NatsError::InvalidOptions`] when `opts` mixes Core and `JetStream` fields incompatibly,
    /// [`NatsError::Subscribe`] when the broker rejects a Core subscription, or
    /// [`NatsError::JetStream`] when the `JetStream` stream or consumer cannot be resolved.
    pub async fn subscribe(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        opts.validate()?;
        if opts.is_jetstream() {
            self.subscribe_jetstream(opts).await
        } else {
            self.subscribe_core(opts).await
        }
    }

    async fn subscribe_core(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let client = self.connected()?;
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
        Ok(NatsSubscriber::from_core(subject, inner))
    }

    async fn subscribe_jetstream(
        &self,
        opts: SubscribeOptions,
    ) -> Result<NatsSubscriber, NatsError> {
        let ctx = async_nats::jetstream::new(self.connected()?);
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
        ))
    }

    /// Returns a publisher bound to this broker.
    ///
    /// It may be created before [`Broker::connect`] (for example inside the `with_broker` builder);
    /// it resolves the shared connection when it first publishes.
    #[must_use]
    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher::new(Arc::clone(&self.client))
    }

    /// Closes the underlying NATS connection. A no-op if the broker never connected.
    pub async fn shutdown_client(&self) {
        if let Some(client) = self.client.get() {
            let _ = client.drain().await;
        }
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;

    async fn connect(&self) -> Result<(), Self::Error> {
        self.client
            .get_or_try_init(|| async {
                let addrs = self.addrs.as_deref().ok_or(NatsError::NotConnected)?;
                async_nats::connect(addrs)
                    .await
                    .map_err(|err| NatsError::Connect(Box::new(err)))
            })
            .await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.shutdown_client().await;
        Ok(())
    }
}

// By-name subscription capability: the runtime's default `Name` source resolves through this for
// the common Core-subject case. The explicit `NatsBroker::` path on the inherent `subscribe`
// disambiguates it from this trait method (inherent methods win, but spell it out for clarity).
#[allow(clippy::use_self)]
impl Subscribe for NatsBroker {
    type Subscriber = NatsSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        NatsBroker::subscribe(self, SubscribeOptions::new(name)).await
    }
}

#[cfg(test)]
mod tests {
    use ruststream::{OutgoingMessage, Publisher};

    use super::*;

    // `new` records the address without connecting, so operations needing the connection fail
    // cleanly until `Broker::connect` runs. No server required.
    #[tokio::test]
    async fn new_does_not_connect() {
        let broker = NatsBroker::new("nats://127.0.0.1:4222");

        let publish_err = broker
            .publisher()
            .publish(OutgoingMessage::new("orders", b"{}".as_slice()))
            .await
            .unwrap_err();
        assert!(matches!(publish_err, NatsError::NotConnected));

        let subscribe_err = broker
            .subscribe(SubscribeOptions::new("orders"))
            .await
            .unwrap_err();
        assert!(matches!(subscribe_err, NatsError::NotConnected));
    }
}
