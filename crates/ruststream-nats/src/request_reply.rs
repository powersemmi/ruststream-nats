//! [`RequestReply`] capability for the NATS publisher.

use async_nats::Request;
use bytes::Bytes;
use ruststream::{OutgoingMessage, RequestReply};
use std::time::Duration;

use crate::{
    convert::headers_to_nats,
    error::NatsError,
    message::{CoreMessage, NatsMessage},
    publisher::NatsPublisher,
};

impl RequestReply for NatsPublisher {
    type Reply = NatsMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let client = self.client_clone()?;
        let subject = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let headers_owned = headers_to_nats(msg.headers());

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
