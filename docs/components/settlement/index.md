---
title: Acknowledgement
description: "A JetStream delivery settles on the server: ack acknowledges it, retry sends a negative acknowledgement, drop terminates it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

A JetStream delivery settles on the server: [`ack`](api:ruststream::runtime::HandlerOutcome::ack) acknowledges it, [`retry`](api:ruststream::runtime::HandlerOutcome::retry) sends a negative acknowledgement, [`drop`](api:ruststream::runtime::HandlerOutcome::drop) terminates it.

A [`JetStreamSubject`](../subscriptions/jetstream-subject.md) reads a stream through a consumer, and the consumer tracks every message it hands out until the message is settled.
The handler's outcome becomes the acknowledgement the server receives.
`ack` sends `+ACK`, and the consumer moves past the message.
`retry` sends `-NAK`, and the consumer delivers the message again at once, with its delivery count raised by one.
`drop` sends `+TERM`, and the server stops delivering the message.
In the manual form `Ok` acknowledges and `Err(outcome)` settles by the outcome.
A delivery nobody settles comes back once the consumer's [`ack_wait`](./ack-wait.md) has passed.
[`retry_after`](./retry-after.md) asks for the next delivery after a delay, and [`max_attempts`](./retries.md) caps how many deliveries one message gets.
A Core NATS subscription has no acknowledgement at all: its deliveries are processed [at most once](./at-most-once.md).

## Example

The first reservation after startup fails while the stock count loads, so its order is delivered again; an order of nothing is terminated.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_settlement_ack.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_settlement_ack.rs#handler"
```

</TabItem>
</Tabs>
