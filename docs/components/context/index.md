---
title: JetStreamContext
description: "JetStreamContext gives a handler the delivery's stream, consumer, sequences, delivery count and pending count, each read through Ctx<K>."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# JetStreamContext

[`JetStreamContext`](api:ruststream_nats::context::JetStreamContext) gives a handler the delivery's stream, consumer, sequences, delivery count and pending count, each read through `Ctx<K>`.

The subscription builds it once per delivery, beside the payload and the headers.
The server writes this metadata on each JetStream delivery; a Core NATS delivery has none.
A handler binds one field with [`Ctx<K>`](api:ruststream::runtime::Ctx), where `K` is a key of [`context::keys`](api:ruststream_nats::context::keys):

| Key | Value | What it is |
| --- | --- | --- |
| [`Stream`](api:ruststream_nats::context::keys::Stream) | `Option<String>` | the stream the message is stored in |
| [`Consumer`](api:ruststream_nats::context::keys::Consumer) | `Option<String>` | the consumer that delivered it, the durable name or the server's name for an ephemeral one |
| [`StreamSequence`](api:ruststream_nats::context::keys::StreamSequence) | `Option<u64>` | the message's sequence in its stream |
| [`ConsumerSequence`](api:ruststream_nats::context::keys::ConsumerSequence) | `Option<u64>` | the delivery's sequence in its consumer |
| [`Delivered`](api:ruststream_nats::context::keys::Delivered) | `Option<i64>` | how many times the server has delivered the message, 1 on the first delivery |
| [`Pending`](api:ruststream_nats::context::keys::Pending) | `Option<u64>` | how many messages the consumer still holds behind this one |

Every key reads `None` on a Core NATS delivery, so one handler works on both kinds of subscription.
The keys are imported from `context::keys`.
A body that reads several fields may take `ctx: &mut Context<'_, JetStreamContext>` instead and call `ctx.context(key)` per field, as the manual form does.
There each key is also a constant, such as `keys::STREAM_SEQUENCE`, and a name reads as `Option<&str>`.
The stream sequence is what a publish states as its [expected last sequence](../publishing/jetstream-publish/expectations.md), and `Delivered` tells a redelivery from a first delivery.

## Example

Each order is written to a ledger with where it sits in the stream `ORDERS`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_context.rs#context"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_context.rs#context"
```

</TabItem>
</Tabs>
