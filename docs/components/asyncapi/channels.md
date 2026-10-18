---
sidebar_position: 2
title: Channels
description: "A JetStream consumer, and a publisher that requires a stream, describe themselves under x-ruststream-jetstream on the channel."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A JetStream consumer, and a publisher that requires a stream, describe themselves under `x-ruststream-jetstream` on the channel.

A channel's `address` is the subject.
A channel read by a [`JetStreamSubject`](../subscriptions/jetstream-subject.md) reports the consumer's settings:

| Field | Value |
|---|---|
| `stream` | the stream the consumer reads |
| `durable` | the durable name, when the descriptor sets one |
| `filterSubject` | the subjects of the stream the consumer reads, the descriptor's subject by default |
| `ackWaitSeconds` | how long the server waits for a settlement |
| `maxAckPending` | how many unacknowledged deliveries the consumer may hold |
| `deliverPolicy` | where a new consumer starts: `all`, `new`, `last`, `last_per_subject`, `by_start_sequence` or `by_start_time` |

A channel a [`JetStreamPublish`](../publishing/jetstream-publish/index.md) with [`expect_stream`](../publishing/jetstream-publish/expect-stream.md) publishes to reports `expectedStream` and `subject`.
The subject is the destination the mount site resolved, the one the stream has to store.
A Core NATS subscription, a JetStream publish that requires no stream and a plain `Publish` add nothing to the channel.

## Example

The channel `orders.stored` reports the durable consumer `archiver` of the stream `ORDERS`.
The channel `receipts.issued` reports that the stream `RECEIPTS` has to store what is published there.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
