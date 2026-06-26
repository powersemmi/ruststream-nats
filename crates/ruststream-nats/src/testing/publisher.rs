//! [`NatsTestPublisher`]: `Publisher` + `RequestReply` on top of the in-memory router.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use ruststream::{OutgoingMessage, Publisher, RequestReply};

use crate::{
    error::NatsError,
    testing::{
        broker::{TestBrokerState, validate_publish_subject},
        router::Delivery,
        subject::SubjectPattern,
        subscriber::NatsTestMessage,
    },
};

static INBOX_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_inbox_subject() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let seq = INBOX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("_INBOX.{nanos:x}{seq:x}")
}

/// Publisher returned by [`crate::testing::NatsTestBroker::publisher`].
#[derive(Clone)]
pub struct NatsTestPublisher {
    state: Arc<TestBrokerState>,
}

impl std::fmt::Debug for NatsTestPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsTestPublisher").finish_non_exhaustive()
    }
}

impl NatsTestPublisher {
    pub(crate) fn new(state: Arc<TestBrokerState>) -> Self {
        Self { state }
    }
}

impl Publisher for NatsTestPublisher {
    type Error = NatsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        validate_publish_subject(msg.name())?;
        self.state.router.publish(
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
            self.state.coordinator().as_ref(),
        );
        Ok(())
    }
}

impl RequestReply for NatsTestPublisher {
    type Reply = NatsTestMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout_dur: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let inbox = new_inbox_subject();
        let pattern = SubjectPattern::parse(&inbox).expect("generated inbox subject must parse");
        let (id, requeue, mut rx) = self.state.router.subscribe(pattern);

        let mut headers = msg.headers().clone();
        headers.insert("reply-to", Bytes::from(inbox.clone()));
        let outgoing =
            OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers.clone());

        if let Err(err) = self.publish(outgoing).await {
            self.state.router.unsubscribe(id);
            return Err(err);
        }

        let received: Option<Delivery> = tokio::time::timeout(timeout_dur, rx.recv())
            .await
            .ok()
            .flatten();
        self.state.router.unsubscribe(id);

        let delivery = received.ok_or(NatsError::RequestTimeout)?;
        Ok(NatsTestMessage::from_delivery(delivery, requeue))
    }
}
