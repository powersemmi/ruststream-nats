---
sidebar_position: 1
title: expect_stream
description: "expect_stream names the stream a publisher's subjects must be stored by; a subject another stream stores, or none, is refused."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# expect_stream

[`expect_stream`](api:ruststream_nats::JetStreamPublish::expect_stream) names the stream a publisher's subjects must be stored by; a subject another stream stores, or none, is refused.

It is part of the policy, so it applies to every message the publisher sends.
Each message carries the stream's name in the `Nats-Expected-Stream` header, and the server compares it with the stream that stores the subject.
A mismatch fails the publish with [`NatsError::JetStream`](api:ruststream_nats::NatsError::JetStream), and nothing is stored.
A subject computed at run time, from a region, a tenant or a configuration value, is where this catches a mistake: a message routed to the wrong stream, or to none, is an error instead of a write somewhere else.
With the `asyncapi` feature the generated document reports the expected stream on the channel, under `x-ruststream-jetstream`.

## Example

Every shipment must be stored by `SHIPMENTS`; a region whose subject that stream does not store is refused, and the order is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_publishing_expect_stream.rs#policy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_publishing_expect_stream.rs#policy"
```

</TabItem>
</Tabs>
