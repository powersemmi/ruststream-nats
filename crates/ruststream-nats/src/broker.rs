//! The [`NatsBroker`]: the entry point of the `async-nats` integration.

use async_nats::jetstream::consumer::{PullConsumer, pull::Config as ConsumerConfig};
use ruststream::Broker;

use crate::{
    error::NatsError, publisher::NatsPublisher, subscribe_options::SubscribeOptions,
    subscriber::NatsSubscriber,
};

/// A NATS broker handle backed by an [`async_nats::Client`].
///
/// Construct with [`NatsBroker::connect`]; the resulting handle is cheap to clone. Subscriptions
/// (Core or `JetStream`) are opened uniformly through [`NatsBroker::subscribe`] with
/// [`SubscribeOptions`]; the broker dispatches internally on whether the options describe a
/// `JetStream` consumer.
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
    client: async_nats::Client,
}

impl std::fmt::Debug for NatsBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsBroker").finish_non_exhaustive()
    }
}

impl NatsBroker {
    /// Connects to a NATS server using the default `async-nats` connection options.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Connect`] when the connection cannot be established.
    pub async fn connect(addrs: impl async_nats::ToServerAddrs) -> Result<Self, NatsError> {
        let client = async_nats::connect(addrs)
            .await
            .map_err(|err| NatsError::Connect(Box::new(err)))?;
        Ok(Self { client })
    }

    /// Wraps an already-constructed `async-nats` client. Useful for advanced configuration
    /// (TLS, credentials, custom options).
    #[must_use]
    pub const fn from_client(client: async_nats::Client) -> Self {
        Self { client }
    }

    /// Returns a clone of the underlying client. Useful for advanced operations not yet covered
    /// by the wrapper, including building a `JetStream` context directly.
    #[must_use]
    pub fn client(&self) -> async_nats::Client {
        self.client.clone()
    }

    /// Opens a subscription described by `opts`. Selects Core or `JetStream` based on whether
    /// [`SubscribeOptions::jetstream`] was called.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::InvalidOptions`] when `opts` mixes Core and `JetStream` fields
    /// incompatibly, [`NatsError::Subscribe`] when the broker rejects a Core subscription, or
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
        let subject = opts.subject().to_owned();
        let inner = if let Some(queue) = opts.queue_group_ref() {
            self.client
                .queue_subscribe(subject.clone(), queue.to_owned())
                .await
                .map_err(|err| NatsError::Subscribe(Box::new(err)))?
        } else {
            self.client
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
        let ctx = async_nats::jetstream::new(self.client.clone());
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
    #[must_use]
    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher::new(self.client.clone())
    }

    /// Closes the underlying NATS connection.
    pub async fn shutdown_client(&self) {
        let _ = self.client.drain().await;
    }
}

impl Broker for NatsBroker {
    type Subscriber = NatsSubscriber;
    type Publisher = NatsPublisher;
    type Error = NatsError;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.shutdown_client().await;
        Ok(())
    }
}
