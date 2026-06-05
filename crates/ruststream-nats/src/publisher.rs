//! Publishes core NATS messages over an `async-nats` client.

use std::sync::Arc;

use bytes::Bytes;
use ruststream::{OutgoingMessage, Publisher};
use tokio::sync::OnceCell;

use crate::{convert::headers_to_nats, error::NatsError};

/// NATS publisher built on top of [`async_nats::Client`]. Cheap to clone.
///
/// Holds the broker's shared connection cell, so a publisher created before the broker connects
/// resolves the client on first use; publishing before [`Broker::connect`](ruststream::Broker::connect)
/// returns [`NatsError::NotConnected`].
#[derive(Clone)]
pub struct NatsPublisher {
    client: Arc<OnceCell<async_nats::Client>>,
}

impl std::fmt::Debug for NatsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsPublisher").finish_non_exhaustive()
    }
}

impl NatsPublisher {
    pub(crate) fn new(client: Arc<OnceCell<async_nats::Client>>) -> Self {
        Self { client }
    }

    pub(crate) fn client_clone(&self) -> Result<async_nats::Client, NatsError> {
        self.client.get().cloned().ok_or(NatsError::NotConnected)
    }
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let client = self.client_clone()?;
        let subject = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let result = match headers_to_nats(msg.headers()) {
            Some(headers) => client.publish_with_headers(subject, headers, payload).await,
            None => client.publish(subject, payload).await,
        };
        result.map_err(|err| NatsError::Publish(Box::new(err)))
    }
}
