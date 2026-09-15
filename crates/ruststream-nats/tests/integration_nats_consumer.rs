//! What a [`JetStreamSubject`] asks the server for, read back from the consumer the server
//! created.
//!
//! Every setting on the descriptor is a field of a `JetStream` consumer, and the server is the
//! only place that can say which value it actually holds. So each test here names a setting,
//! opens the subscription, and asserts twice: the consumer's own configuration carries the value,
//! and the deliveries behave the way that value promises. A descriptor that names nothing is
//! asserted the same way, because the defaults this crate documents are its own and differ from
//! the server's.
//!
//! Skipped unless `NATS_TEST_URL` is set (see `integration_nats.rs` for how to run).

use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::Info as ConsumerInfo;
use async_nats::jetstream::stream::Config as StreamConfig;
use futures::{Stream, StreamExt};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber, nonzero,
};
use ruststream_nats::{
    ConnectedNatsBroker, DeliverPolicy, JetStreamSubject, NatsBroker, NatsError, NatsMessage,
    NatsPublish, NonZeroDuration,
};
use tokio::time::timeout;

mod live;

/// How long a delivery that is expected may take to arrive.
const WAIT: Duration = Duration::from_secs(5);
/// How long a delivery that must not arrive is waited for before the subject is called quiet.
const IDLE: Duration = Duration::from_millis(400);
/// The pause between two reads of the server's own state while waiting for it to settle.
const POLL: Duration = Duration::from_millis(20);

/// The defaults this crate documents for a descriptor that names no window and no cap. The cap
/// differs from the server's own default of 1000, which is what makes it visible in the consumer
/// the server created.
const DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_ACK_PENDING: i64 = 1024;

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default()
}

/// A live connection plus a stream of its own over `ruststream.consumer.<prefix>.<unique>.>`,
/// created here and deleted on teardown.
struct JetStreamFixture {
    connected: ConnectedNatsBroker,
    base: String,
    stream: String,
}

impl JetStreamFixture {
    /// `None` skips the test when there is no reachable server.
    async fn open(prefix: &str) -> Option<Self> {
        let url = live::url("NATS_TEST_URL")?;
        let connected = match NatsBroker::new(url.as_str()).connect().await {
            Ok(connected) => connected,
            Err(err) => {
                live::unreachable(&url, &err);
                return None;
            }
        };
        let suffix = unique_suffix();
        let base = format!("ruststream.consumer.{prefix}.{suffix}");
        let stream = format!("RS_IT_CONSUMER_{}_{suffix}", prefix.to_uppercase());
        connected
            .jetstream()
            .create_stream(StreamConfig {
                name: stream.clone(),
                subjects: vec![format!("{base}.>")],
                ..Default::default()
            })
            .await
            .expect("create_stream failed");
        Some(Self {
            connected,
            base,
            stream,
        })
    }

    fn subject(&self, leaf: &str) -> String {
        format!("{}.{leaf}", self.base)
    }

    /// A descriptor reading `<base>.<leaf>` from this fixture's stream.
    fn consumer(&self, leaf: &str) -> JetStreamSubject {
        JetStreamSubject::new(self.subject(leaf), self.stream.clone())
    }

    async fn publish(&self, leaf: &str, payload: &[u8]) {
        self.connected
            .publisher(NatsPublish)
            .publish(
                OutgoingMessage::new(self.subject(leaf).as_str(), payload),
                None,
            )
            .await
            .expect("publish failed");
    }

    /// What the server says about the consumer named `durable`.
    async fn consumer_info(&self, durable: &str) -> ConsumerInfo {
        self.connected
            .jetstream()
            .get_stream(&self.stream)
            .await
            .expect("get_stream failed")
            .consumer_info(durable)
            .await
            .expect("consumer_info failed")
    }

    /// The server's view of the consumer once `ready` holds, or a failure naming what was
    /// waited for. The consumer's counters move when the server acts, so they are polled rather
    /// than slept on.
    async fn until_consumer(
        &self,
        durable: &str,
        what: &str,
        ready: impl Fn(&ConsumerInfo) -> bool + Send + Sync,
    ) -> ConsumerInfo {
        timeout(WAIT, async {
            loop {
                let info = self.consumer_info(durable).await;
                if ready(&info) {
                    return info;
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the consumer never reported {what}"))
    }

    async fn teardown(self) {
        let _ = self.connected.jetstream().delete_stream(&self.stream).await;
        self.connected.shutdown().await.expect("shutdown failed");
    }
}

/// The next delivery on `stream`, or a failure naming what was waited for.
async fn next_delivery<S>(stream: &mut S, within: Duration) -> NatsMessage
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    timeout(within, stream.next())
        .await
        .expect("timed out waiting for a delivery")
        .expect("the subscription stream ended")
        .expect("the subscription reported an error")
}

/// Asserts that nothing arrives on `stream` within [`IDLE`].
async fn stays_quiet<S>(stream: &mut S, why: &str)
where
    S: Stream<Item = Result<NatsMessage, NatsError>> + Unpin,
{
    if let Ok(Some(Ok(msg))) = timeout(IDLE, stream.next()).await {
        panic!(
            "{why}, but a delivery of {:?} arrived",
            String::from_utf8_lossy(msg.payload())
        );
    }
}

// Every setting the descriptor carries is a field of the consumer the server created, so the
// server is asked what it holds. The negative half is the descriptor that names nothing: the
// defaults it gets are this crate's, and the cap makes that visible, since a server left to
// itself writes 1000.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_settings_a_descriptor_names_reach_the_consumer_the_server_created() {
    let Some(fx) = JetStreamFixture::open("config").await else {
        return;
    };

    let named = fx
        .consumer("named")
        .durable("it-configured")
        .filter_subject(fx.subject("named"))
        .ack_wait(NonZeroDuration::from_secs(nonzero!(2)))
        .max_ack_pending(7)
        .deliver_policy(DeliverPolicy::New);
    let subscriber = fx
        .connected
        .subscribe_with(named)
        .await
        .expect("consumer create failed");

    let config = fx.consumer_info("it-configured").await.config;
    assert_eq!(config.durable_name.as_deref(), Some("it-configured"));
    assert_eq!(config.filter_subject, fx.subject("named"));
    assert_eq!(config.ack_wait, Duration::from_secs(2));
    assert_eq!(config.max_ack_pending, 7);
    assert_eq!(config.deliver_policy, DeliverPolicy::New);
    drop(subscriber);

    let plain = fx.consumer("plain").durable("it-default");
    let subscriber = fx
        .connected
        .subscribe_with(plain)
        .await
        .expect("consumer create failed");

    let config = fx.consumer_info("it-default").await.config;
    assert_eq!(
        config.ack_wait, DEFAULT_ACK_WAIT,
        "a descriptor that names no window gets the documented 30 seconds",
    );
    assert_eq!(
        config.max_ack_pending, DEFAULT_MAX_ACK_PENDING,
        "a descriptor that names no cap gets this crate's 1024, not the server's own default",
    );
    assert_eq!(config.deliver_policy, DeliverPolicy::All);
    assert_eq!(
        config.filter_subject,
        fx.subject("plain"),
        "the filter defaults to the subject the descriptor was constructed with",
    );
    drop(subscriber);

    fx.teardown().await;
}

// The delivery policy decides where a newly created consumer starts, which is only observable
// against a stream that already holds something: `New` must skip what is there and `All` must
// replay it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_consumer_starts_where_the_deliver_policy_says() {
    let Some(fx) = JetStreamFixture::open("policy").await else {
        return;
    };
    fx.publish("events", b"before").await;

    let mut fresh = fx
        .connected
        .subscribe_with(
            fx.consumer("events")
                .durable("it-only-new")
                .deliver_policy(DeliverPolicy::New),
        )
        .await
        .expect("consumer create failed");
    {
        let mut stream = std::pin::pin!(fresh.stream());
        stays_quiet(
            &mut stream,
            "a consumer starting at New must skip the stream's history",
        )
        .await;

        fx.publish("events", b"after").await;
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            msg.payload(),
            b"after",
            "the first delivery must be the message published after the consumer was created",
        );
        msg.ack().await.expect("ack failed");
    }
    drop(fresh);

    // The same stream, read by a consumer that names no policy: the default replays everything.
    let mut replay = fx
        .connected
        .subscribe_with(fx.consumer("events").durable("it-replays"))
        .await
        .expect("consumer create failed");
    {
        let mut stream = std::pin::pin!(replay.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            msg.payload(),
            b"before",
            "the default policy replays the stream from its first message",
        );
        msg.ack().await.expect("ack failed");
    }
    drop(replay);

    fx.teardown().await;
}

// The ack window is the server's own timer: a delivery left unsettled comes back once it runs
// out, and the server counts that as a redelivery. What is timed is the redelivery arriving, so
// no sleep takes part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unacknowledged_delivery_comes_back_after_the_ack_wait() {
    const ACK_WAIT: Duration = Duration::from_secs(1);

    let Some(fx) = JetStreamFixture::open("ackwait").await else {
        return;
    };
    fx.publish("slow", b"unsettled").await;

    let mut consumer = fx
        .connected
        .subscribe_with(
            fx.consumer("slow")
                .durable("it-ack-wait")
                .ack_wait(NonZeroDuration::from_secs(nonzero!(1))),
        )
        .await
        .expect("consumer create failed");

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let first = next_delivery(&mut stream, WAIT).await;
        assert_eq!(first.redelivery_count(), Some(1));
        let delivered_at = Instant::now();
        // Dropped without settling: the window is what brings it back, not a negative
        // acknowledgement.
        drop(first);

        let again = next_delivery(&mut stream, WAIT).await;
        let elapsed = delivered_at.elapsed();
        assert!(
            elapsed >= ACK_WAIT,
            "the redelivery came back after {elapsed:?}, before the {ACK_WAIT:?} window elapsed",
        );
        assert_eq!(again.payload(), b"unsettled");
        assert_eq!(
            again.redelivery_count(),
            Some(2),
            "an expired window counts as a redelivery, exactly as a nack does",
        );
        again.ack().await.expect("ack failed");
    }

    let info = fx
        .until_consumer("it-ack-wait", "the redelivery it made", |info| {
            info.num_redelivered > 0 || info.ack_floor.stream_sequence > 0
        })
        .await;
    assert!(
        info.config.ack_wait == ACK_WAIT,
        "the window the descriptor named must be the consumer's own",
    );

    drop(consumer);
    fx.teardown().await;
}

// A filter narrows a consumer to part of its stream. The stream keeps both messages, so the one
// outside the filter is proved absent from the subscription rather than absent from the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filtered_consumer_reads_only_the_subject_its_filter_names() {
    let Some(fx) = JetStreamFixture::open("filter").await else {
        return;
    };

    let mut consumer = fx
        .connected
        .subscribe_with(
            JetStreamSubject::new(format!("{}.>", fx.base), fx.stream.clone())
                .durable("it-filtered")
                .filter_subject(fx.subject("kept")),
        )
        .await
        .expect("consumer create failed");

    fx.publish("skipped", b"not for this consumer").await;
    fx.publish("kept", b"for this consumer").await;

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"for this consumer");
        msg.ack().await.expect("ack failed");
        stays_quiet(
            &mut stream,
            "the filtered-out subject must not reach this consumer",
        )
        .await;
    }

    let info = fx
        .until_consumer("it-filtered", "the acknowledgement it took", |info| {
            info.num_ack_pending == 0
        })
        .await;
    assert_eq!(
        info.num_pending, 0,
        "the consumer has nothing left to deliver, though the stream still holds both messages",
    );
    let stream_messages = fx
        .connected
        .jetstream()
        .get_stream(&fx.stream)
        .await
        .expect("get_stream failed")
        .info()
        .await
        .expect("stream info failed")
        .state
        .messages;
    assert_eq!(
        stream_messages, 2,
        "the filter is the consumer's, so the stream keeps the message it filtered out",
    );

    drop(consumer);
    fx.teardown().await;
}

// The cap on in-flight unacknowledged deliveries is the server's back-pressure: with one allowed,
// a second message waits for the first to be acknowledged. Both halves are asserted, so a cap
// that never reached the server would fail here rather than pass by arriving in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_ack_pending_caps_the_deliveries_in_flight() {
    let Some(fx) = JetStreamFixture::open("pending").await else {
        return;
    };
    fx.publish("queue", b"first").await;
    fx.publish("queue", b"second").await;

    let mut consumer = fx
        .connected
        .subscribe_with(
            fx.consumer("queue")
                .durable("it-one-in-flight")
                .max_ack_pending(1),
        )
        .await
        .expect("consumer create failed");

    {
        let mut stream = std::pin::pin!(consumer.stream());
        let first = next_delivery(&mut stream, WAIT).await;
        assert_eq!(first.payload(), b"first");
        stays_quiet(
            &mut stream,
            "one unacknowledged delivery fills a cap of one, so the second must wait",
        )
        .await;

        let info = fx.consumer_info("it-one-in-flight").await;
        assert_eq!(
            info.num_ack_pending, 1,
            "the server holds one delivery open"
        );
        assert_eq!(info.num_pending, 1, "and keeps the other waiting");

        first.ack().await.expect("ack failed");
        let second = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            second.payload(),
            b"second",
            "the acknowledgement frees the cap and the next message follows",
        );
        second.ack().await.expect("ack failed");
    }

    drop(consumer);
    fx.teardown().await;
}

// A durable name is what makes the server keep a consumer's position, so a subscription that
// opens under the same name resumes rather than replays. A different name is a different
// consumer, and it replays from the start - which is what proves the first one resumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_consumer_resumes_where_it_left_off() {
    let Some(fx) = JetStreamFixture::open("durable").await else {
        return;
    };
    fx.publish("log", b"one").await;

    let mut consumer = fx
        .connected
        .subscribe_with(fx.consumer("log").durable("it-durable"))
        .await
        .expect("consumer create failed");
    {
        let mut stream = std::pin::pin!(consumer.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(msg.payload(), b"one");
        msg.ack().await.expect("ack failed");
    }
    drop(consumer);

    let info = fx
        .until_consumer("it-durable", "the acknowledgement it took", |info| {
            info.ack_floor.stream_sequence == 1
        })
        .await;
    assert_eq!(
        info.config.durable_name.as_deref(),
        Some("it-durable"),
        "the consumer the server keeps is the named one",
    );

    fx.publish("log", b"two").await;
    let mut resumed = fx
        .connected
        .subscribe_with(fx.consumer("log").durable("it-durable"))
        .await
        .expect("consumer create failed");
    {
        let mut stream = std::pin::pin!(resumed.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            msg.payload(),
            b"two",
            "the named consumer resumes past what it acknowledged",
        );
        msg.ack().await.expect("ack failed");
    }
    drop(resumed);

    let mut fresh = fx
        .connected
        .subscribe_with(fx.consumer("log").durable("it-fresh"))
        .await
        .expect("consumer create failed");
    {
        let mut stream = std::pin::pin!(fresh.stream());
        let msg = next_delivery(&mut stream, WAIT).await;
        assert_eq!(
            msg.payload(),
            b"one",
            "a consumer under another name has no position to resume from",
        );
        msg.ack().await.expect("ack failed");
    }
    drop(fresh);

    fx.teardown().await;
}
