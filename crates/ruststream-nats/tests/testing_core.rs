//! Integration tests for the handler-stub NATS test broker.
//!
//! Drives the public surface (`NatsTestBroker`, `NatsTestPublisher`, `NatsTestSubscriber`,
//! `NatsTestClient`) without going through any harness, to keep failures localised. `JetStream`
//! edge-case semantics live in `tests/integration_nats.rs` against a real NATS server.

#![cfg(feature = "testing")]

use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::{
    BatchSubscriber, Broker, DescribeServer, Headers, IncomingMessage, OutgoingMessage,
    Partitioned, Publisher, RequestReply, Subscriber, testing::TestClient,
};
use ruststream_nats::{
    NatsError, PARTITION_KEY_HEADER, SubscribeOptions,
    testing::{NatsTestBroker, NatsTestClient, NatsTestMessage},
};

const WAIT: Duration = Duration::from_secs(1);

async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<NatsTestMessage, NatsError>> + Unpin,
{
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    let payload = msg.payload().to_vec();
    msg.ack().await.expect("ack");
    payload
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_round_trip_through_broker_traits() {
    let broker = NatsTestBroker::new();
    broker.connect().await.expect("connect");

    let mut subscriber = broker
        .subscribe(SubscribeOptions::new("orders.created"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders.created", b"o1"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let got = next_payload(&mut stream).await;
    assert_eq!(got, b"o1");
    drop(stream);

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_validates_subjects() {
    let broker = NatsTestBroker::new();
    let publisher = broker.publisher();
    let err = publisher
        .publish(OutgoingMessage::new("orders.*", b"x"))
        .await
        .expect_err("wildcard subject must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("publish"),
        "expected NatsError::Publish, got {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_subscription_receives_matching_subjects() {
    let broker = NatsTestBroker::new();
    let mut star_sub = broker
        .subscribe(SubscribeOptions::new("orders.*"))
        .await
        .expect("subscribe *");
    let mut tail_sub = broker
        .subscribe(SubscribeOptions::new(">"))
        .await
        .expect("subscribe >");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders.created", b"a"))
        .await
        .expect("publish a");
    publisher
        .publish(OutgoingMessage::new("orders.updated", b"b"))
        .await
        .expect("publish b");
    publisher
        .publish(OutgoingMessage::new("payments.captured", b"c"))
        .await
        .expect("publish c");

    let mut star_stream = Box::pin(star_sub.stream());
    let star1 = next_payload(&mut star_stream).await;
    let star2 = next_payload(&mut star_stream).await;
    assert_eq!(&star1, b"a");
    assert_eq!(&star2, b"b");

    let mut tail_stream = Box::pin(tail_sub.stream());
    let tail1 = next_payload(&mut tail_stream).await;
    let tail2 = next_payload(&mut tail_stream).await;
    let tail3 = next_payload(&mut tail_stream).await;
    assert_eq!(&tail1, b"a");
    assert_eq!(&tail2, b"b");
    assert_eq!(&tail3, b"c");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_redelivers_to_same_subscriber() {
    let broker = NatsTestBroker::new();
    let mut subscriber = broker
        .subscribe(SubscribeOptions::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"once"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("first delivery")
        .expect("stream has next")
        .expect("ok");
    first.nack(true).await.expect("nack requeue");

    let second = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("redelivery")
        .expect("stream has next")
        .expect("ok");
    assert_eq!(second.payload(), b"once");
    second.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_reply_round_trip() {
    let broker = NatsTestBroker::new();
    let mut responder = broker
        .subscribe(SubscribeOptions::new("echo"))
        .await
        .expect("subscribe echo");
    let responder_publisher = broker.publisher();

    // Background responder: read one request and publish to its reply-to subject.
    let responder_task = tokio::spawn(async move {
        let mut stream = Box::pin(responder.stream());
        let req = stream
            .next()
            .await
            .expect("stream has next")
            .expect("delivery ok");
        let reply_to = req
            .headers()
            .reply_to()
            .expect("request carries reply-to header")
            .to_owned();
        let payload = format!("reply:{}", String::from_utf8_lossy(req.payload()));
        req.ack().await.expect("ack");
        let reply = OutgoingMessage::new(reply_to.as_str(), payload.as_bytes());
        responder_publisher.publish(reply).await.expect("reply");
    });

    let publisher = broker.publisher();
    let reply = publisher
        .request(
            OutgoingMessage::new("echo", b"hello"),
            Duration::from_secs(1),
        )
        .await
        .expect("reply within timeout");
    assert_eq!(reply.payload(), b"reply:hello");

    responder_task.await.expect("responder task joined");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_times_out_when_no_responder() {
    let broker = NatsTestBroker::new();
    let publisher = broker.publisher();
    let err = publisher
        .request(
            OutgoingMessage::new("echo.absent", b"hi"),
            Duration::from_millis(50),
        )
        .await
        .expect_err("must time out");
    assert!(format!("{err}").contains("timed out"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_are_propagated_to_subscribers() {
    let broker = NatsTestBroker::new();
    let mut subscriber = broker
        .subscribe(SubscribeOptions::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert("correlation-id", "abc-1");
    let outgoing = OutgoingMessage::new("orders", b"{}").with_headers(headers);
    publisher.publish(outgoing).await.expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("stream has next")
        .expect("ok");
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().correlation_id(), Some("abc-1"));
    msg.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_client_drives_expect_published() {
    let client = NatsTestClient::start().await.expect("start");
    TestClient::publish(&client, "events", b"first")
        .await
        .expect("publish first");
    TestClient::publish(&client, "events", b"second")
        .await
        .expect("publish second");
    let observed = client
        .expect_published("events", 2, Duration::from_secs(1))
        .await
        .expect("expect_published");
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
    client.shutdown().await.expect("shutdown");
}

// Regression: 0.2 took the receiver out of an Option in `stream()` and panicked on the second
// call, while the Subscriber contract (and the conformance helpers, which re-enter `stream()`
// per call) allow re-entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_can_be_reentered() {
    let broker = NatsTestBroker::new();
    let mut subscriber = broker
        .subscribe(SubscribeOptions::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"one"))
        .await
        .expect("publish one");
    {
        let mut stream = Box::pin(subscriber.stream());
        let got = next_payload(&mut stream).await;
        assert_eq!(got, b"one");
    }

    publisher
        .publish(OutgoingMessage::new("orders", b"two"))
        .await
        .expect("publish two");
    let mut stream = Box::pin(subscriber.stream());
    let got = next_payload(&mut stream).await;
    assert_eq!(got, b"two");
}

#[tokio::test]
async fn describe_server_returns_nats_protocol() {
    let broker = NatsTestBroker::new();
    // The testing broker does not connect to a server, but describe_server should still return
    // a spec with "nats" as the protocol.
    let spec = broker.describe_server();
    assert_eq!(spec.protocol, "nats");
}

#[tokio::test]
async fn partition_key_header_is_surfaced() {
    let client = NatsTestClient::start().await.expect("start");
    let mut sub = client.subscribe("events").await.expect("subscribe");

    let mut headers = Headers::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");

    client
        .publisher()
        .await
        .expect("publisher")
        .publish(OutgoingMessage::new("events", b"payload").with_headers(headers))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("item")
        .expect("ok");

    assert_eq!(msg.partition_key(), Some(b"tenant-a".as_slice()));
    msg.ack().await.ok();
    client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn batch_subscriber_yields_non_empty_batches() {
    let client = NatsTestClient::start().await.expect("start");
    let publisher = client.publisher().await.expect("publisher");

    // Open the subscription before publishing so messages are buffered.
    let mut sub = client.subscribe("batch").await.expect("subscribe");

    for i in 0u8..5 {
        publisher
            .publish(OutgoingMessage::new("batch", &[i]))
            .await
            .expect("publish");
    }

    let mut batches = Box::pin(sub.batches());
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");

    assert!(!batch.is_empty(), "batch must contain at least one message");
    // Ack each message in the batch.
    for msg in batch {
        msg.ack().await.ok();
    }
    client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn partition_key_absent_yields_none() {
    let client = NatsTestClient::start().await.expect("start");
    let mut sub = client.subscribe("events.bare").await.expect("subscribe");

    client
        .publisher()
        .await
        .expect("publisher")
        .publish(OutgoingMessage::new("events.bare", b"payload"))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("item")
        .expect("ok");

    assert_eq!(msg.partition_key(), None);
    msg.ack().await.ok();
    client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn batch_drains_in_publish_order() {
    let client = NatsTestClient::start().await.expect("start");
    let publisher = client.publisher().await.expect("publisher");
    let mut sub = client.subscribe("batch.order").await.expect("subscribe");

    let count = 5u8;
    for i in 0..count {
        publisher
            .publish(OutgoingMessage::new("batch.order", &[i]))
            .await
            .expect("publish");
    }

    let mut batches = Box::pin(sub.batches());
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");

    assert!(batch.len() <= usize::from(count));
    for (i, msg) in batch.into_iter().enumerate() {
        assert_eq!(msg.payload(), &[u8::try_from(i).expect("count fits u8")]);
        msg.ack().await.ok();
    }
    client.shutdown().await.expect("shutdown");
}

// Same re-entry contract as `stream()`: dropping the batch stream and calling `batches()` again
// must keep working.
#[tokio::test]
async fn batches_can_be_reentered() {
    let client = NatsTestClient::start().await.expect("start");
    let publisher = client.publisher().await.expect("publisher");
    let mut sub = client.subscribe("batch.reenter").await.expect("subscribe");

    publisher
        .publish(OutgoingMessage::new("batch.reenter", b"one"))
        .await
        .expect("publish");
    {
        let mut batches = Box::pin(sub.batches());
        let batch = tokio::time::timeout(WAIT, batches.next())
            .await
            .expect("batch within timeout")
            .expect("stream has next")
            .expect("ok batch");
        assert_eq!(
            batch.first().map(|m| m.payload().to_vec()),
            Some(b"one".to_vec()),
        );
        for msg in batch {
            msg.ack().await.ok();
        }
    }

    publisher
        .publish(OutgoingMessage::new("batch.reenter", b"two"))
        .await
        .expect("publish");
    let mut batches = Box::pin(sub.batches());
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");
    assert_eq!(
        batch.first().map(|m| m.payload().to_vec()),
        Some(b"two".to_vec()),
    );
    for msg in batch {
        msg.ack().await.ok();
    }
    client.shutdown().await.expect("shutdown");
}
