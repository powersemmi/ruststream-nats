---
sidebar_position: 6
description: "Retry a NATS delivery now or later, cap the attempts and keep what keeps failing in a dead-letter stream, on a stream, a subject or a pattern."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a delivery now or later, caps the attempts and keeps what keeps failing in a [dead-letter](../components/settlement/dead-letters.md) stream, on a stream consumer, a Core subject and a pattern.

NATS has no dead-letter mechanism.
JetStream can cap the deliveries of a message, but it sends the last one nowhere.
So the mount site declares the cap and the dead-letter subject, and the framework applies them to both models.

The example is a payments service that settles payments and sends refunds through a bank.
It keeps its dead letters in a stream of their own:

```bash
nats stream add PAYMENTS --subjects 'payments.settle' --defaults
nats stream add DEAD --subjects 'dead.*' --defaults
```

## Retrying on a stream

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#settle"
```

</TabItem>
</Tabs>

`outcome` turns the bank's refusal into the handler's outcome.
A busy bank is asked again in 30 seconds with [`retry_after`](../components/settlement/retry-after.md).
On a JetStream consumer the delay travels in the negative acknowledgement: the server holds the payment and delivers it again on the same consumer, with the same stream sequence.
Nothing is published, and a process that stops in between loses nothing.
A silent bank is asked again at once with `retry`, and a declined card is dropped.

## The cap and the dead letters

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) counts every delivery of one payment, the first included.
On a JetStream consumer the count is the server's own, so an [`ack_wait`](../components/settlement/ack-wait.md) that ran out counts too.
The fifth delivery is not retried: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) publishes it to `dead.payments`, payload and headers as they arrived.
A dropped payment settles and goes nowhere, so the dead letters hold only the payments that ran out of attempts.
A capped consumer without a dead-letter subject terminates its last delivery.

`dead.payments` is a plain publish, and Core NATS keeps nothing nobody reads.
The `DEAD` stream stores it, so a dead letter waits until someone looks at it.
No subject the app reads matches a dead-letter subject, or a spent delivery would come back to the handler that gave up on it.

## Reading the dead letters

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` reads the dead payments from the `DEAD` stream like any other stream and logs each for a person to look at.

## Retrying on Core subjects

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refunds"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refunds"
```

</TabItem>
</Tabs>

Core NATS cannot hold a message back.
On a Core subscription a retry is a copy of the delivery that the framework publishes: `retry` publishes it at once, `retry_after` when the delay ends.
The copy carries the delivery count in its [`x-ruststream-retry-count`](api:ruststream::runtime::RETRY_COUNT_HEADER) header, which the cap reads.
A delayed copy is at most once: a process that stops before the delay ends loses it.

A [`CoreSubject`](../components/subscriptions/core-subject.md) publishes its copies to its own subject.
`send_refund` reads the pattern `refunds.*`, which has no single subject to publish to.
So the app names one with [`out_retry(Publish).to("refunds.retry")`](../components/settlement/out-retry.md), a subject the pattern matches, and the copies come back to `send_refund`.
A registration over a pattern without it refuses to start, whether its handler retries or not.
The third delivery of a refund goes to `dead.refunds`, with its count in the header.

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
[Testing a NATS service](testing.md) covers the harness.
