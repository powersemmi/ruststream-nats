---
sidebar_position: 2
title: publish_ack
description: "publish_ack returns the stream's acknowledgement: the stream, the sequence, and whether the deduplication window saw a repeat."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# publish_ack

[`publish_ack`](api:ruststream_nats::JetStreamPublisher::publish_ack) returns the stream's acknowledgement: the stream, the sequence, and whether the deduplication window saw a repeat.

The acknowledgement is a [`PublishAck`](api:ruststream_nats::PublishAck): `stream` names the stream that stored the message, `sequence` is its position there, and `duplicate` is true when the stream had already stored a message with the same [`message_id`](./message-id.md).
A plain publish through the same publisher waits for the same acknowledgement and discards it.
The method belongs to the live [`JetStreamPublisher`](api:ruststream_nats::JetStreamPublisher), which a startup hook receives: `b.after_startup(JetStreamPublish::default(), hook)` pairs the policy and hands the publisher to the hook.
It takes the message as bytes and, as its second argument, the [`JetStreamOptions`](api:ruststream_nats::JetStreamOptions) of this one message, or `None`.
A refused message returns [`NatsError::JetStream`](api:ruststream_nats::NatsError::JetStream).
The call is not cancel-safe: a future dropped after the message left loses the acknowledgement, and the message may or may not be stored.

## Example

A startup hook seeds the first order and logs where the stream stored it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_jetstream.rs#publish_ack"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_jetstream.rs#publish_ack"
```

</TabItem>
</Tabs>
