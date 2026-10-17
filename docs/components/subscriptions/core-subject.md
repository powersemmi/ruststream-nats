---
sidebar_position: 1
title: CoreSubject
description: "CoreSubject reads one Core NATS subject; it stores nothing, and a delayed copy of a delivery is published back to the subject itself."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# CoreSubject

[`CoreSubject`](api:ruststream_nats::CoreSubject) reads one Core NATS subject; it stores nothing, and a delayed copy of a delivery is published back to the subject itself.

[`CoreSubject::new`](api:ruststream_nats::CoreSubject::new) takes the subject, and its one setting is the [queue group](./queue-group.md).
The server delivers a message to every subscription on the subject at that moment and keeps nothing for later.
A subject is published to and subscribed to under one name, so a delayed retry needs nothing from the mount site.
A [`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) settles the delivery, and after the delay the runtime publishes a copy back to the subject.
The copy waits in the service's memory, so a process that exits before the timer fires loses it.
A subject with `*` or `>` in it is a pattern, which is [`CoreWildcard`](./core-wildcard.md); `CoreSubject` given one stops the service at startup.
An empty subject stops it too.
A stream that keeps messages for a consumer is [`JetStreamSubject`](./jetstream-subject.md).

## Example

A payment the bank has not cleared yet comes back to `payments.captured` after 30 seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_core.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_core.rs#descriptor"
```

</TabItem>
</Tabs>
