---
sidebar_position: 8
description: "Test a NATS service in process with TestApp: the production app, streams, replies, delays, and the same test against a NATS server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a NATS service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process NATS](../components/testing/index.md) or against a server.

The service under test charges card payments.
A payment arrives on `card.payments`, which the stream `CARD_PAYMENTS` stores, and a durable consumer reads it.
The handler replies with a receipt on `card.receipts`.
A payment without a card is dropped.
A payment over the bank's instant limit is held: the handler asks for it again in a minute with [`retry_after`](../components/settlement/retry-after.md).
After three deliveries a payment goes to `card.review`, the [dead-letter subject](../components/settlement/dead-letters.md).
The app is built by `service(broker)`, so a test chooses the broker's address and `main` uses the production one.
The tests need the `testing` feature of `ruststream-nats` in `[dev-dependencies]`.

## A payment and its receipt

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`NatsBroker`](../components/broker/index.md) to an in-process NATS.
The address is never dialled, and no stream has to be created first.
The test addresses the broker by its production type, `tb.broker::<NatsBroker>()`.
`message(..).publish()` publishes the payment to the subject its type declares, and the stream stores it.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `card.receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## A payment without a card

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#declined"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#declined"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
The consumer holds the payment as a server does and delivers it again when the delay ends.
The third delivery is the last one the mount site allows, so the payment goes to `card.review` as it arrived.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process NATS models

The in-process NATS reads what the client would read: the addresses, and the connection options that change what the server delivers (`no_echo`, the inbox prefix).
It routes and stores as a server does:

- Subjects match with `*` and `>`, and a queue group hands each message to one member.
- A stream keeps sequences, its duplicate window and the expectations a publish states.
- A consumer starts where its deliver policy says and carries the metadata of the [JetStream context](../components/context/index.md).
- An acknowledgement, a negative one with or without a delay, and a termination act as on a server; a delivery left unsettled comes back after `ack_wait`.
- A request reaches the app's responder, and a request nobody reads gets "no responders" at once.

It refuses what the client or the server refuses: a subject with whitespace, a header with no NATS form, a message over 1 MiB, a JetStream publish no stream takes.
A stream there stores what the app's consumers read, and a subscription finds every stream it names.
A stream's own subject list and limits, `max_ack_pending`, authentication and TLS need a server.

## The same test on a server

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to a running NATS server.
The test reads the address from `NATS_TEST_URL` with `live::url`, a helper of the example's own, and skips when it is unset.
A stream is the server's state, so the test creates `CARD_PAYMENTS` on a connection of its own first and deletes it afterwards.
Deleting the stream also deletes its consumer, so the next run starts clean.
The assertions are the in-process ones, unchanged.

```bash
cargo test
NATS_TEST_URL=nats://localhost:4222 cargo test -- --test-threads=1
```
