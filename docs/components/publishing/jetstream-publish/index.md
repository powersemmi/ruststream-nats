---
title: JetStreamPublish
description: "JetStreamPublish stores each message in a stream and waits for the stream's acknowledgement, so a refused publish is an error."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# JetStreamPublish

[`JetStreamPublish`](api:ruststream_nats::JetStreamPublish) stores each message in a stream and waits for the stream's acknowledgement, so a refused publish is an error.

A mount site names it like the plain policy: `.out_reply(JetStreamPublish::default())`, `.out(Slot, JetStreamPublish::default())`, or `b.after_startup(..)`.
The runtime pairs it into a [`JetStreamPublisher`](api:ruststream_nats::JetStreamPublisher) at startup.
Every publish waits for the stream that stores the subject to acknowledge the message.
A subject no stream stores, a violated expectation or a storage failure fails the publish with [`NatsError::JetStream`](api:ruststream_nats::NatsError::JetStream).
So the publish costs a round trip to the server, and the handler learns whether the message was stored.
A plain [`Publish`](../index.md) to a subject a stream stores is stored too, but the publisher does not learn whether it was.
The policy can name the stream it expects with [`expect_stream`](./expect-stream.md).
One message states its own settings with publish steps: a deduplication id with [`message_id`](./message-id.md) and the position it expects the stream at with the [expectations](./expectations.md).
[`publish_ack`](./publish-ack.md) returns the acknowledgement itself.
A reply can be stored in a stream while the rest of the service publishes over Core NATS: each mount site chooses its policy.

## Example

The confirmation an order is answered with is stored in a stream of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_jetstream.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_jetstream.rs#reply"
```

</TabItem>
</Tabs>

The mount site names the JetStream policy for the reply, for the archive slot and for the startup publish.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_jetstream.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_jetstream.rs#app"
```

</TabItem>
</Tabs>
