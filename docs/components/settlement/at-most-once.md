---
sidebar_position: 1
title: At most once
description: "Core NATS stores nothing and acknowledges nothing, so a Core delivery is processed at most once and a retry delivers nothing again."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# At most once

Core NATS stores nothing and acknowledges nothing, so a Core delivery is processed at most once and a retry delivers nothing again.

The server hands a Core message to the subscribers connected at that moment and keeps no copy.
So settling a delivery of a [`CoreSubject`](../subscriptions/core-subject.md) or a [`CoreWildcard`](../subscriptions/core-wildcard.md) reports [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs it as a warning naming the subscription.
The handler's answer still decides what the framework does next.
`ack` and `drop` end the delivery.
A plain `retry` ends it too: the server has nothing to deliver again, and a message the handler could not process is gone.
[`retry_after`](./retry-after.md), or any retry under a [cap](./retries.md), makes the framework publish a copy of the message back to the subject, so the message comes back once.
That copy waits in the service's memory, so a process that exits before the delay is over loses it.
A message that has to survive a failed handler or a restart is read from a stream through a [`JetStreamSubject`](../subscriptions/jetstream-subject.md), which settles on the server.

## Example

A shipping notice whose mail server does not answer asks for a retry, and the message is not delivered again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_settlement_core.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_settlement_core.rs#handler"
```

</TabItem>
</Tabs>
