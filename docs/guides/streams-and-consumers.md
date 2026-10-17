---
sidebar_position: 3
description: "Read a JetStream stream through durable and ephemeral pull consumers, choose where a consumer starts, read a delivery's metadata and take batches."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Streams and consumers

This guide reads a JetStream stream through durable and ephemeral consumers, chooses where a new consumer starts, reads the metadata of a delivery and takes messages in batches.

A stream stores the messages published to its subjects, each under a sequence number.
A consumer reads a stream at its own pace and acknowledges message by message.

The example is a shop.
It fulfils new orders, shows every order event on a dashboard, and reconciles paid orders with the bank.

## The stream

A stream is the server's state.
The service finds it at startup and never creates it, so create it once with the NATS command-line tool:

```bash
nats stream add ORDERS --subjects 'orders.*' --defaults
```

`ORDERS` stores every message published to `orders.created`, `orders.paid` and any other subject that `orders.*` matches.
A plain publish to such a subject is stored, whoever publishes it.
A subscription to a stream the server does not have fails the service's start.

## A durable consumer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#fulfil"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#fulfil"
```

</TabItem>
</Tabs>

[`JetStreamSubject`](../components/subscriptions/jetstream-subject.md) takes the subject the consumer reads and the stream that stores it.
The subject is the consumer's filter, so `fulfil` reads only `orders.created` out of `ORDERS`.
[`filter_subject`](../components/subscriptions/filter-subject.md) sets another filter where the two should differ.
The service creates the consumer when it starts, with every setting of the descriptor.

[`durable`](../components/subscriptions/durable.md) names the consumer `fulfilment`.
The server keeps its position, so a restarted service continues where it stopped.
Replicas of the service share the one durable consumer, and each message goes to one of them.
That is the JetStream counterpart of a [queue group](subjects-and-queue-groups.md).

The handler reads the delivery's place in the stream with [`Ctx<StreamSequence>`](../components/context/index.md).
The [JetStream context](../components/context/index.md) also carries the stream and consumer names, the consumer sequence, the delivery count and the number of messages still pending.
In the manual form the handler takes `Context<'_, JetStreamContext>` and reads the same keys with `ctx.context(..)`.
On a Core NATS delivery every key reads `None`.

## An ephemeral consumer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#dashboard"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#dashboard"
```

</TabItem>
</Tabs>

A descriptor without `durable` creates an ephemeral consumer.
Each process gets its own, and the server removes it after the subscription closes.
So every replica of the service shows every order event.

[`DeliverPolicy`](../components/subscriptions/deliver-policy.md) says where a new consumer starts.
`All`, the default, starts at the first message the stream holds, and `New` at the next message published.
`Last`, `LastPerSubject`, `ByStartSequence` and `ByStartTime` start at the last message, the last message of each subject, a sequence number or a point in time.
The policy applies once, when the consumer is created; after that a durable consumer resumes from its own position.

## Batches

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#reconcile"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#reconcile"
```

</TabItem>
</Tabs>

A handler that takes `&[Order]` gets a [batch](../components/subscriptions/batches.md) of messages and runs once for the whole batch.
The returned outcome settles every message of it.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#app"
```

</TabItem>
</Tabs>

The mount site names the batch size: `batch(100)` asks the server for up to a hundred messages in one pull.
[`pull_expires`](api:ruststream_nats::JetStreamSubject::pull_expires) on the descriptor says how long a pull waits for them, here one second, after which a smaller batch is handed over.
The default is five seconds.

The reply of `fulfil` is published over Core NATS, the broker's default.
[Publishing to streams](publishing-to-streams.md) stores replies in a stream instead.
Core and JetStream subscriptions share one connection, so one service mixes both models freely.

[`max_ack_pending`](../components/subscriptions/max-ack-pending.md) caps how many deliveries a consumer holds unacknowledged; the default is 1024.
[Acknowledgement and delivery guarantees](acknowledgement.md) covers how each delivery settles.
