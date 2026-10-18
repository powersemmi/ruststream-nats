---
sidebar_position: 8
title: max_ack_pending
description: "max_ack_pending caps how many unacknowledged deliveries the server lets a consumer hold at once; the default is 1024."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# max_ack_pending

[`max_ack_pending`](api:ruststream_nats::JetStreamSubject::max_ack_pending) caps how many unacknowledged deliveries the server lets a consumer hold at once; the default is 1024.

A delivery is pending from the moment the server hands it out until it is settled.
Once the consumer holds that many, the server stops delivering until a settlement frees a place.
The cap belongs to the consumer, so every subscription that shares a [durable](./durable.md) counts against one cap.
It bounds the work in flight from the server's side, whatever the service does.
`workers(n)` runs up to `n` handlers at once in one subscription; with a cap below `n`, the cap is the limit.
A delivery the consumer holds counts against its [`ack_wait`](../settlement/ack-wait.md) from the moment it is handed out, so a large cap over slow handlers lets deliveries wait out their `ack_wait` and come back.
The cap is enforced by the server, so a test of it runs against a live server.

## Example

Six workers render reports, and the server hands out two at a time.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_pending.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_pending.rs#descriptor"
```

</TabItem>
</Tabs>
