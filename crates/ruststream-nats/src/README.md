NATS broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging framework,
backed by [`async-nats`](https://docs.rs/async-nats): Core NATS subjects and `JetStream` streams
over one connection.

Core NATS hands a message to whoever is subscribed at that moment and stores nothing. `JetStream`
stores it in a stream, which a pull consumer reads at its own pace and acknowledges message by
message. Both models run on the same connection, so a service chooses per handler and per publish.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the application
object, and the attribute writes the `main`:

```
# mod demo {
use ruststream_nats::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`NatsBroker::new`] only records the configuration; the runtime
connects the broker at startup and drains it when the service stops.

# Where things are

The [guides][guides] take a NATS service from installation to production, in this order:
[getting started][g-start], [subjects, wildcards and queue groups][g-subjects],
[streams and consumers][g-streams], [acknowledgement and delivery guarantees][g-ack],
[publishing to streams][g-publishing], [retries and dead letters][g-retries],
[request-reply][g-request-reply], [testing][g-testing] and
[connecting to production][g-production]. The [architecture][architecture] pages show how the
crate sits between `async-nats` and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [NatsBroker][c-broker]: [`NatsBroker`], connected into [`ConnectedNatsBroker`] and closed into
  [`ClosedNatsBroker`].
- [ConnectOptions][c-connect-options]: [`NatsBroker::with_options`] attaches the `async-nats`
  connection options.
- [Security][c-security]: credentials and TLS as connection options.
- [Startup][c-startup]: the connect within the client's `connection_timeout`, and each
  subscription opened against the server.
- [Shutdown][c-shutdown]: the drain, and [`NatsError::Closed`] for a publisher used afterwards.
- [Prelude][c-prelude]: [`prelude`], the one glob a NATS service imports.
- [Errors][c-errors]: [`NatsError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare subject in `#[subscriber]` reads that Core NATS
  subject.
- [CoreSubject][c-core-subject]: [`CoreSubject`], one Core NATS subject.
- [CoreWildcard][c-core-wildcard]: [`CoreWildcard`], every subject a `*` or `>` pattern matches.
- [queue_group][c-queue-group]: [`CoreSubject::queue_group`], one member of a group takes each
  message.
- [JetStreamSubject][c-jetstream-subject]: [`JetStreamSubject`], a stream read through a pull
  consumer.
- [durable][c-durable]: [`JetStreamSubject::durable`], a named consumer that keeps its position.
- [filter_subject][c-filter-subject]: [`JetStreamSubject::filter_subject`], the subjects of the
  stream a consumer reads.
- [DeliverPolicy][c-deliver-policy]: [`DeliverPolicy`], where a new consumer starts.
- [max_ack_pending][c-max-ack-pending]: [`JetStreamSubject::max_ack_pending`], the cap on
  unacknowledged deliveries.
- [Batches][c-batches]: a slice handler over [`NatsSubscriber`] batches, sized at the mount site
  and timed by [`JetStreamSubject::pull_expires`].
- [Keys][c-keys]: [`PARTITION_KEY_HEADER`] keys the lanes of `workers(n, by_key)`.

## Settlement

- [Acknowledgement][c-settlement]: what an `ack`, a `retry` and a `drop` do on a `JetStream`
  delivery.
- [ack_wait][c-ack-wait]: [`JetStreamSubject::ack_wait`], how long the server waits for a
  settlement.
- [At most once][c-at-most-once]: a Core NATS delivery, which nothing acknowledges.
- [Retries][c-retries]: `max_attempts`, counted by the server on `JetStream` and in a header on
  Core NATS.
- [retry_after][c-retry-after]: a delay the server holds on `JetStream`, and a copy the framework
  publishes on Core NATS.
- [Dead letters][c-dead-letters]: `dead_letter`, the subject a spent delivery goes to.
- [out_retry][c-out-retry]: the subject a [`CoreWildcard`] registration sends its delayed copies
  to.

## Context

- [JetStreamContext][c-context]: [`JetStreamContext`](context::JetStreamContext) and the keys in
  [`context::keys`]: stream, consumer, sequences, delivery count and pending count of a delivery.

## Publishing

- [NatsPublish][c-publishing]: [`NatsPublish`], the plain Core NATS policy, and its live
  [`NatsPublisher`].
- [Headers][c-headers]: what a header must be to travel over NATS.
- [JetStreamPublish][c-jetstream-publish]: [`JetStreamPublish`] and its live
  [`JetStreamPublisher`], which wait for the stream's acknowledgement.
- [expect_stream][c-expect-stream]: [`JetStreamPublish::expect_stream`], the stream a
  publisher's subjects must be stored by.
- [message_id][c-message-id]: [`JetStreamPublishSteps::message_id`], a deduplication id carried
  in [`JetStreamOptions`].
- [Expectations][c-expectations]: the `expect_last_*` steps of [`JetStreamPublishSteps`].
- [publish_ack][c-publish-ack]: [`JetStreamPublisher::publish_ack`] and its [`PublishAck`].

## Request-reply

- [RequestReply][c-request-reply]: `request` on [`NatsPublisher`], answered on an inbox.
- [Responders][c-responders]: a handler that answers on the inbox of each request.

## Tooling

- [NATS bindings][c-asyncapi]: the [server][c-asyncapi-server],
  [channels][c-asyncapi-channels], [operations][c-asyncapi-operations] and
  [replies][c-asyncapi-replies] of the generated `AsyncAPI` document (feature `asyncapi`).
- [Testing][c-testing]: the in-process NATS server
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running server.

# Cargo features

Both are off by default and additive. `testing` adds the in-process mode `TestApp::start`
connects the broker through, and `asyncapi` contributes the NATS bindings to the generated
document. The codecs, the macros and the harness itself are features of the core crate.

[guides]: https://powersemmi.github.io/ruststream-nats/latest/
[g-start]: https://powersemmi.github.io/ruststream-nats/latest/guides/getting-started/
[g-subjects]: https://powersemmi.github.io/ruststream-nats/latest/guides/subjects-and-queue-groups/
[g-streams]: https://powersemmi.github.io/ruststream-nats/latest/guides/streams-and-consumers/
[g-ack]: https://powersemmi.github.io/ruststream-nats/latest/guides/acknowledgement/
[g-publishing]: https://powersemmi.github.io/ruststream-nats/latest/guides/publishing-to-streams/
[g-retries]: https://powersemmi.github.io/ruststream-nats/latest/guides/retries/
[g-request-reply]: https://powersemmi.github.io/ruststream-nats/latest/guides/request-reply/
[g-testing]: https://powersemmi.github.io/ruststream-nats/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-nats/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-nats/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-nats/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/
[c-connect-options]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/connect-options/
[c-security]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/security/
[c-startup]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-nats/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/
[c-core-subject]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/core-subject/
[c-core-wildcard]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/core-wildcard/
[c-queue-group]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/queue-group/
[c-jetstream-subject]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/jetstream-subject/
[c-durable]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/durable/
[c-filter-subject]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/filter-subject/
[c-deliver-policy]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/deliver-policy/
[c-max-ack-pending]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/max-ack-pending/
[c-batches]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/batches/
[c-keys]: https://powersemmi.github.io/ruststream-nats/latest/components/subscriptions/keys/
[c-settlement]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/
[c-ack-wait]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/ack-wait/
[c-at-most-once]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/at-most-once/
[c-retries]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/retries/
[c-retry-after]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/retry-after/
[c-dead-letters]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/dead-letters/
[c-out-retry]: https://powersemmi.github.io/ruststream-nats/latest/components/settlement/out-retry/
[c-context]: https://powersemmi.github.io/ruststream-nats/latest/components/context/
[c-publishing]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/
[c-headers]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/headers/
[c-jetstream-publish]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/jetstream-publish/
[c-expect-stream]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/jetstream-publish/expect-stream/
[c-message-id]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/jetstream-publish/message-id/
[c-expectations]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/jetstream-publish/expectations/
[c-publish-ack]: https://powersemmi.github.io/ruststream-nats/latest/components/publishing/jetstream-publish/publish-ack/
[c-request-reply]: https://powersemmi.github.io/ruststream-nats/latest/components/request-reply/
[c-responders]: https://powersemmi.github.io/ruststream-nats/latest/components/request-reply/responders/
[c-asyncapi]: https://powersemmi.github.io/ruststream-nats/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-nats/latest/components/asyncapi/server/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-nats/latest/components/asyncapi/channels/
[c-asyncapi-operations]: https://powersemmi.github.io/ruststream-nats/latest/components/asyncapi/operations/
[c-asyncapi-replies]: https://powersemmi.github.io/ruststream-nats/latest/components/asyncapi/replies/
[c-testing]: https://powersemmi.github.io/ruststream-nats/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-nats/latest/components/testing/live/
