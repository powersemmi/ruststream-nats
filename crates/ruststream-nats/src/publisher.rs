//! Publishes core NATS messages over an `async-nats` client.

use bytes::Bytes;
use ruststream::{OutgoingMessage, Publisher};

use crate::{convert::headers_to_nats, error::NatsError};

/// NATS publisher built on top of [`async_nats::Client`]. Cheap to clone.
#[derive(Clone)]
pub struct NatsPublisher {
    client: async_nats::Client,
}

impl std::fmt::Debug for NatsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsPublisher").finish_non_exhaustive()
    }
}

impl NatsPublisher {
    pub(crate) const fn new(client: async_nats::Client) -> Self {
        Self { client }
    }

    pub(crate) fn client_clone(&self) -> async_nats::Client {
        self.client.clone()
    }
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let subject = msg.topic().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let result = match headers_to_nats(msg.headers()) {
            Some(headers) => {
                self.client
                    .publish_with_headers(subject, headers, payload)
                    .await
            }
            None => self.client.publish(subject, payload).await,
        };
        result.map_err(|err| NatsError::Publish(Box::new(err)))
    }
}
