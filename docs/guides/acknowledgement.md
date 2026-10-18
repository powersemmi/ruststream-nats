---
sidebar_position: 4
description: "Settle JetStream deliveries with the handler's outcome, size ack_wait for slow handlers, handle redeliveries, and know what Core NATS does not promise."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement and delivery guarantees

This guide settles JetStream deliveries with the handler's outcome, sizes `ack_wait` for slow handlers, handles a message delivered twice, and shows what Core NATS does not promise.

A handler settles its delivery with the value it returns, and the runtime sends the settlement after the handler.
On a JetStream consumer each outcome is an acknowledgement the server acts on:

| Outcome | Sent to the server | What the server does |
|---|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | `+ACK` | forgets the message for this consumer |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | `-NAK` | delivers it again at once |
| [`retry_after(delay)`](../components/settlement/retry-after.md) | `-NAK` with the delay | holds it and delivers it again after the delay |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | `+TERM` | never delivers it again |
| no settlement | nothing | delivers it again after [`ack_wait`](../components/settlement/ack-wait.md) |

A redelivery keeps the message's stream sequence, and the server counts its deliveries.

The example is a payments service.
It captures authorized payments through the bank and announces each capture to a dashboard.

## Settling a payment

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#capture"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#capture"
```

</TabItem>
</Tabs>

The bank answers in four ways, and each needs a different outcome.
A capture returns `Captured`, which the runtime publishes as the reply and then acknowledges the payment.
`Busy` means the bank's rate limit is used up, so the payment waits 30 seconds on the server.
`Unavailable` means the bank did not answer, so the payment comes back at once.
`Declined` means the card cannot be charged, and another try gets the same answer, so the payment is terminated.
A `retry` without a cap comes back forever; [Retries and dead letters](retries.md) adds the cap and a dead-letter subject.

## Messages delivered twice

JetStream delivers at least once.
A payment comes back after a `retry`, after `ack_wait` ran out, and after a process stopped before it settled.
So a handler sees some payments twice, and its work has to accept a repeat.
Here the bank takes the payment id as an idempotency key and charges a repeated capture once.
[`Ctx<Delivered>`](../components/context/index.md) is the server's delivery count, 1 on the first delivery, and `capture` logs a payment it sees again.

## Slow handlers and `ack_wait`

The server waits [`ack_wait`](../components/settlement/ack-wait.md) for a settlement, 30 seconds unless the descriptor names another.
After that it delivers the message again, while the first handler may still run.
`capture` sets 60 seconds, above the longest time the bank takes to answer.
Set `ack_wait` above the worst time of the handler, and the two deliveries never overlap.
An expired `ack_wait` also counts as a delivery for the [attempt cap](retries.md).

## The dashboard on Core NATS

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#dashboard"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#dashboard"
```

</TabItem>
</Tabs>

`refresh` reads the captures on a Core NATS subject.
[Core NATS](../components/settlement/at-most-once.md) delivers at most once and has no acknowledgement.
Settling a Core delivery reports [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs it as a warning.
A Core message the process loses is gone, and nothing brings it back.
The dashboard can lose an update, because the next capture refreshes it again.
A payment cannot, so payments travel through a stream.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

The bank's client is the app's state, opened once at startup.
A batch handler settles its whole batch with one outcome, or each message on its own when it returns `Vec<HandlerOutcome>`.

The tests in the example file run the app in process with a paused clock, so the 30 seconds of `Busy` pass at once.
