//! [`NatsTestClient`]: `TestClient` driver consumed by the conformance harness.

use std::time::Duration;

use ruststream::{Broker, OutgoingMessage, Publisher, RawMessage, testing::TestClient};

use crate::{
    error::NatsError,
    subscribe_options::SubscribeOptions,
    testing::{broker::NatsTestBroker, subscriber::NatsTestSubscriber},
};

/// Driver around a single [`NatsTestBroker`] instance.
///
/// `NatsTestClient::start()` constructs a fresh, isolated broker. Use it as the entry point in
/// the `ruststream-conformance` harness and in handler integration tests.
pub struct NatsTestClient {
    broker: NatsTestBroker,
}

impl std::fmt::Debug for NatsTestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsTestClient")
            .field("broker", &self.broker)
            .finish()
    }
}

impl TestClient for NatsTestClient {
    type Broker = NatsTestBroker;
    type Error = NatsError;

    async fn start() -> Result<Self, Self::Error> {
        Ok(Self {
            broker: NatsTestBroker::new(),
        })
    }

    fn broker(&self) -> &Self::Broker {
        &self.broker
    }

    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let publisher = self.broker.publisher();
        publisher
            .publish(OutgoingMessage::new(topic, payload))
            .await
    }

    async fn subscribe(&self, topic: &str) -> Result<NatsTestSubscriber, Self::Error> {
        self.broker.subscribe(SubscribeOptions::new(topic)).await
    }

    async fn publisher(&self) -> Result<<Self::Broker as Broker>::Publisher, Self::Error> {
        Ok(self.broker.publisher())
    }

    async fn expect_published(
        &self,
        topic: &str,
        count: usize,
        timeout_dur: Duration,
    ) -> Result<Vec<RawMessage>, Self::Error> {
        Ok(self
            .broker
            .state()
            .router
            .expect_published(topic, count, timeout_dur)
            .await)
    }

    async fn shutdown(self) -> Result<(), Self::Error> {
        <NatsTestBroker as Broker>::shutdown(&self.broker).await
    }
}
