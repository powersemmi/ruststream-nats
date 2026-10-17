---
sidebar_position: 3
title: message_id
description: "message_id tags a publish with Nats-Msg-Id, so a repeat inside the stream's deduplication window is stored once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# message_id

[`message_id`](api:ruststream_nats::JetStreamPublishSteps::message_id) tags a publish with `Nats-Msg-Id`, so a repeat inside the stream's deduplication window is stored once.

The stream remembers the ids it stored for its deduplication window, two minutes unless the stream is configured otherwise.
A second message with the same id inside the window is acknowledged and not stored, and its acknowledgement reports `duplicate`.
An id derived from the input, such as `order-{id}`, makes a redelivered input publish the same message again without storing it twice.
The id describes one message, so the handler body states it as a step on the publish builder.
The step comes from [`JetStreamPublishSteps`](api:ruststream_nats::JetStreamPublishSteps) in this crate's prelude, and the body bounds its slot `Out<impl Publisher<Options = JetStreamOptions>, Slot>`.
That bound ties the body to JetStream, and its signature says so.
The step writes into the call's [`JetStreamOptions`](api:ruststream_nats::JetStreamOptions); the publish still leaves through the mount site's policy and codec.
A test reads the value back with `tb.out::<Slot>().with_options(&JetStreamOptions { .. })`.

## Example

Every shipped order is archived under an id derived from the order, so a redelivered order is stored once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_jetstream.rs#message_id"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_jetstream.rs#message_id"
```

</TabItem>
</Tabs>
