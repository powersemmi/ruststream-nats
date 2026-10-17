---
sidebar_position: 4
title: JetStreamSubject
description: "JetStreamSubject reads a stream through a pull consumer; the stream exists on the server before the service starts."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# JetStreamSubject

[`JetStreamSubject`](api:ruststream_nats::JetStreamSubject) reads a stream through a pull consumer; the stream exists on the server before the service starts.

A stream stores the messages published to its subjects, and a consumer reads them at its own pace.
[`JetStreamSubject::new(subject, stream)`](api:ruststream_nats::JetStreamSubject::new) takes the subject to read and the name of the stream that stores it.
At startup the subscription finds the stream and creates its consumer; a stream the server does not have stops the service.
The crate creates no stream: the service's deployment does, for example with `nats stream add`.
Without [`durable`](./durable.md) the consumer is ephemeral, and the server discards it once the subscription closes.
Each delivery [settles on the server](../settlement/index.md): an `ack` acknowledges it, and a retry or a delay is a negative acknowledgement the server acts on.
The settings and their defaults:

| Setting | Default | What it does |
| --- | --- | --- |
| [`durable`](./durable.md) | none, an ephemeral consumer | names the consumer, which keeps its position across restarts |
| [`filter_subject`](./filter-subject.md) | the subject | narrows what the consumer reads from the stream |
| [`deliver_policy`](./deliver-policy.md) | `DeliverPolicy::All` | where a new consumer starts |
| [`max_ack_pending`](./max-ack-pending.md) | 1024 | how many deliveries the consumer holds unacknowledged |
| [`ack_wait`](../settlement/ack-wait.md) | 30 seconds | how long the server waits for a settlement before it delivers again |
| [`pull_expires`](./batches.md) | 5 seconds | how long one fetch of a batch waits for messages |

The subscription's name is the subject the descriptor names.
A delivery carries its place in the stream, which a handler reads through the [context keys](../context/index.md).

## Example

The stream `ORDERS` stores `orders.*`, and an ephemeral consumer reads it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_jetstream.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_jetstream.rs#descriptor"
```

</TabItem>
</Tabs>
