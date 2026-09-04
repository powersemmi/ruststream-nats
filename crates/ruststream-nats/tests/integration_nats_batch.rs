//! `JetStream` batch semantics of [`ruststream::BatchSubscriber`] against a real NATS server.
//!
//! The runtime has no batch dispatch for `JetStream` pull consumers, and the in-memory testing
//! broker deliberately does not simulate `JetStream` pull semantics, so driving `batches()`
//! directly against a live server is what covers the fetch loop.
//!
//! Skipped unless `NATS_TEST_URL` is set (see `integration_nats.rs` for how to run).

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use futures::StreamExt;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, nonzero,
};
use ruststream_nats::{ConnectedNatsBroker, NatsBroker, NatsPublish, SubscribeOptions};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default()
}

struct JetStreamFixture {
    connected: ConnectedNatsBroker,
    subject: String,
    stream: String,
}

/// Connects to the test server and creates a uniquely named stream; `None` skips the test when
/// `NATS_TEST_URL` is unset or the server is unreachable.
async fn jetstream_fixture(prefix: &str) -> Option<JetStreamFixture> {
    let url = std::env::var("NATS_TEST_URL").ok()?;
    let connected = match NatsBroker::new(url.as_str()).connect().await {
        Ok(connected) => connected,
        Err(err) => {
            eprintln!("could not reach NATS at {url}: {err}; skipping");
            return None;
        }
    };
    let suffix = unique_suffix();
    let subject = format!("ruststream.it.batch.{prefix}.{suffix}");
    let stream = format!("RS_IT_BATCH_{}_{suffix}", prefix.to_uppercase());
    connected
        .jetstream()
        .create_stream(StreamConfig {
            name: stream.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .expect("create_stream failed");
    Some(JetStreamFixture {
        connected,
        subject,
        stream,
    })
}

impl JetStreamFixture {
    fn consumer_options(&self, expires: Duration) -> SubscribeOptions {
        SubscribeOptions::new(self.subject.clone())
            .jetstream(self.stream.clone())
            .filter_subject(self.subject.clone())
            .pull_expires(expires)
    }

    async fn teardown(self) {
        let _ = self.connected.jetstream().delete_stream(&self.stream).await;
        self.connected.shutdown().await.expect("shutdown failed");
    }
}

// The registration's batch size reaches the wire: it is the pull request's batch size, so a run
// longer than one batch comes back as several capped batches rather than one long one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_batch_size_caps_the_pull_batch() {
    let Some(fx) = jetstream_fixture("cap").await else {
        return;
    };

    let publisher = fx.connected.publisher(NatsPublish);
    let total = 7u8;
    for i in 0..total {
        publisher
            .publish(OutgoingMessage::new(fx.subject.as_str(), &[i]))
            .await
            .expect("publish failed");
    }

    let mut consumer = fx
        .connected
        .subscribe_with(fx.consumer_options(Duration::from_millis(300)))
        .await
        .expect("consumer create failed");

    let mut received = Vec::new();
    {
        let mut batches = std::pin::pin!(consumer.batches(nonzero!(3)));
        while received.len() < usize::from(total) {
            let batch = timeout(WAIT, batches.next())
                .await
                .expect("timed out waiting for batch")
                .expect("stream ended")
                .expect("batch error");
            assert!(
                batch.len() <= 3,
                "batch of {} exceeds the size of 3 it was opened with",
                batch.len()
            );
            for msg in batch {
                received.push(msg.payload().to_vec());
                msg.ack().await.expect("ack failed");
            }
        }
    }

    let expected: Vec<Vec<u8>> = (0..total).map(|i| vec![i]).collect();
    assert_eq!(received, expected, "messages must arrive in publish order");

    drop(consumer);
    fx.teardown().await;
}

// The JetStream batch loop retries empty fetches instead of yielding empty batches; a message
// published only after at least one `pull_expires` window has elapsed must still come through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_skip_empty_fetches() {
    let Some(fx) = jetstream_fixture("empty").await else {
        return;
    };

    let mut consumer = fx
        .connected
        .subscribe_with(fx.consumer_options(Duration::from_millis(150)))
        .await
        .expect("consumer create failed");

    let publisher = fx.connected.publisher(NatsPublish);
    let subject = fx.subject.clone();
    let publish_task = tokio::spawn(async move {
        // Longer than pull_expires, so the first fetch comes back empty and is retried.
        tokio::time::sleep(Duration::from_millis(400)).await;
        publisher
            .publish(OutgoingMessage::new(subject.as_str(), b"late"))
            .await
            .expect("publish failed");
    });

    {
        let mut batches = std::pin::pin!(consumer.batches(nonzero!(10)));
        let batch = timeout(WAIT, batches.next())
            .await
            .expect("timed out waiting for batch")
            .expect("stream ended")
            .expect("batch error");
        publish_task.await.expect("publish task failed");

        assert_eq!(batch.len(), 1, "only the late message must be delivered");
        for msg in batch {
            assert_eq!(msg.payload(), b"late");
            msg.ack().await.expect("ack failed");
        }
    }

    drop(consumer);
    fx.teardown().await;
}

// Same re-entry contract as `stream()`: dropping the batch stream and calling `batches()` again
// must keep working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_can_be_reentered() {
    let Some(fx) = jetstream_fixture("reenter").await else {
        return;
    };

    let publisher = fx.connected.publisher(NatsPublish);
    publisher
        .publish(OutgoingMessage::new(fx.subject.as_str(), b"one"))
        .await
        .expect("publish failed");

    let mut consumer = fx
        .connected
        .subscribe_with(fx.consumer_options(Duration::from_millis(300)))
        .await
        .expect("consumer create failed");

    {
        let mut batches = std::pin::pin!(consumer.batches(nonzero!(10)));
        let batch = timeout(WAIT, batches.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("batch error");
        assert_eq!(batch.len(), 1);
        for msg in batch {
            assert_eq!(msg.payload(), b"one");
            msg.ack().await.expect("ack failed");
        }
    }

    publisher
        .publish(OutgoingMessage::new(fx.subject.as_str(), b"two"))
        .await
        .expect("publish failed");
    {
        let mut batches = std::pin::pin!(consumer.batches(nonzero!(10)));
        let batch = timeout(WAIT, batches.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("batch error");
        assert_eq!(batch.len(), 1);
        for msg in batch {
            assert_eq!(msg.payload(), b"two");
            msg.ack().await.expect("ack failed");
        }
    }

    drop(consumer);
    fx.teardown().await;
}
