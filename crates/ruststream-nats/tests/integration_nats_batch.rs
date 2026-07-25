//! `JetStream` batch semantics of [`ruststream::BatchSubscriber`] against a real NATS server.
//!
//! Temporary trait-level coverage: the runtime has no batch dispatch yet
//! (powersemmi/ruststream#21), and the in-memory testing broker deliberately does not simulate
//! `JetStream` pull semantics, so driving `batches()` by hand against a live server is the only
//! way to test the fetch loop. Once #21 lands, rewrite these as app-level tests in
//! `integration_nats.rs` and delete this file.
//!
//! Skipped unless `NATS_TEST_URL` is set (see `integration_nats.rs` for how to run).

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{BatchSubscriber, IncomingMessage, OutgoingMessage, Publisher};
use ruststream_nats::{NatsBroker, SubscribeOptions};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default()
}

struct JetStreamFixture {
    broker: NatsBroker,
    subject: String,
    stream: String,
}

/// Connects to the test server and creates a uniquely named stream; `None` skips the test when
/// `NATS_TEST_URL` is unset or the server is unreachable.
async fn jetstream_fixture(prefix: &str) -> Option<JetStreamFixture> {
    let url = std::env::var("NATS_TEST_URL").ok()?;
    let broker = match NatsBroker::connect(url.as_str()).await {
        Ok(broker) => broker,
        Err(err) => {
            eprintln!("could not reach NATS at {url}: {err}; skipping");
            return None;
        }
    };
    let suffix = unique_suffix();
    let subject = format!("ruststream.it.batch.{prefix}.{suffix}");
    let stream = format!("RS_IT_BATCH_{}_{suffix}", prefix.to_uppercase());
    async_nats::jetstream::new(broker.client())
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .expect("create_stream failed");
    Some(JetStreamFixture {
        broker,
        subject,
        stream,
    })
}

impl JetStreamFixture {
    async fn teardown(self) {
        let _ = async_nats::jetstream::new(self.broker.client())
            .delete_stream(&self.stream)
            .await;
        self.broker.shutdown_client().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_batch_caps_batch_size() {
    let Some(fx) = jetstream_fixture("cap").await else {
        return;
    };

    let publisher = fx.broker.publisher();
    let total = 7u8;
    for i in 0..total {
        publisher
            .publish(OutgoingMessage::new(fx.subject.as_str(), &[i]))
            .await
            .expect("publish failed");
    }

    let mut consumer = fx
        .broker
        .subscribe(
            SubscribeOptions::new(fx.subject.clone())
                .jetstream(fx.stream.clone())
                .filter_subject(fx.subject.clone())
                .pull_batch(NonZeroUsize::new(3).unwrap())
                .pull_expires(Duration::from_millis(300)),
        )
        .await
        .expect("consumer create failed");

    let mut batches = std::pin::pin!(consumer.batches());
    let mut received = Vec::new();
    while received.len() < usize::from(total) {
        let batch = timeout(WAIT, batches.next())
            .await
            .expect("timed out waiting for batch")
            .expect("stream ended")
            .expect("batch error");
        assert!(
            batch.len() <= 3,
            "batch of {} exceeds the pull_batch cap of 3",
            batch.len()
        );
        for msg in batch {
            received.push(msg.payload().to_vec());
            msg.ack().await.expect("ack failed");
        }
    }

    let expected: Vec<Vec<u8>> = (0..total).map(|i| vec![i]).collect();
    assert_eq!(received, expected, "messages must arrive in publish order");

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
        .broker
        .subscribe(
            SubscribeOptions::new(fx.subject.clone())
                .jetstream(fx.stream.clone())
                .filter_subject(fx.subject.clone())
                .pull_batch(NonZeroUsize::new(10).unwrap())
                .pull_expires(Duration::from_millis(150)),
        )
        .await
        .expect("consumer create failed");

    let publisher = fx.broker.publisher();
    let subject = fx.subject.clone();
    let publish_task = tokio::spawn(async move {
        // Longer than pull_expires, so the first fetch comes back empty and is retried.
        tokio::time::sleep(Duration::from_millis(400)).await;
        publisher
            .publish(OutgoingMessage::new(subject.as_str(), b"late"))
            .await
            .expect("publish failed");
    });

    let mut batches = std::pin::pin!(consumer.batches());
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

    fx.teardown().await;
}

// Same re-entry contract as `stream()`: dropping the batch stream and calling `batches()` again
// must keep working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_can_be_reentered() {
    let Some(fx) = jetstream_fixture("reenter").await else {
        return;
    };

    let publisher = fx.broker.publisher();
    publisher
        .publish(OutgoingMessage::new(fx.subject.as_str(), b"one"))
        .await
        .expect("publish failed");

    let mut consumer = fx
        .broker
        .subscribe(
            SubscribeOptions::new(fx.subject.clone())
                .jetstream(fx.stream.clone())
                .filter_subject(fx.subject.clone())
                .pull_batch(NonZeroUsize::new(10).unwrap())
                .pull_expires(Duration::from_millis(300)),
        )
        .await
        .expect("consumer create failed");

    {
        let mut batches = std::pin::pin!(consumer.batches());
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
    let mut batches = std::pin::pin!(consumer.batches());
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

    fx.teardown().await;
}
