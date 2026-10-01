//! [`RequestReply`] capability for the NATS publisher.

// Without the `testing` feature a connection link has one variant; see `broker.rs`.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use async_nats::Request;
use bytes::BytesMut;
use ruststream::{OutgoingMessage, RequestReply};
use std::time::Duration;

use crate::{
    broker::Link,
    convert::nats_parts,
    error::NatsError,
    message::{CoreMessage, NatsMessage},
    publisher::{NatsPublisher, client_for},
};

impl RequestReply for NatsPublisher {
    type Reply = NatsMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let connection = match self.link() {
            Link::Nats(connection) => connection,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => {
                let (subject, payload, headers) = msg.into_parts();
                let reply = bus
                    .request(subject, payload.freeze(), &headers, timeout)
                    .await?;
                return Ok(NatsMessage::Core(Box::new(CoreMessage::new(reply))));
            }
        };
        let client = client_for(connection, msg.name())?;
        let (subject, payload, headers_owned) = nats_parts(msg)?;

        let fut = async {
            let request = match headers_owned {
                Some(headers) => Request::new().payload(payload).headers(headers),
                None => Request::new().payload(payload),
            };
            client
                .send_request(subject, request)
                .await
                .map_err(|err| NatsError::Publish(Box::new(err)))
        };

        let response = tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| NatsError::RequestTimeout)??;
        Ok(NatsMessage::Core(Box::new(CoreMessage::new(response))))
    }
}
