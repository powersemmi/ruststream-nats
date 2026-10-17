---
title: Subscriptions
description: "A subject in #[subscriber] reads that Core NATS subject; a descriptor carries every other setting of its delivery model."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A subject in `#[subscriber]` reads that Core NATS subject; a descriptor carries every other setting of its delivery model.

`#[subscriber("orders.created")]` reads the subject `orders.created` over Core NATS, with no queue group.
It is the same subscription as [`CoreSubject::new("orders.created")`](./core-subject.md).
Core NATS hands a message to whoever is subscribed at that moment and stores nothing, so a message published before the service starts never reaches it.
Every other setting belongs to a descriptor, one type per delivery model:

| Descriptor | Reads | Its settings |
| --- | --- | --- |
| [`CoreSubject`](./core-subject.md) | one Core NATS subject | [`queue_group`](./queue-group.md) |
| [`CoreWildcard`](./core-wildcard.md) | every Core NATS subject a pattern matches | [`queue_group`](./queue-group.md) |
| [`JetStreamSubject`](./jetstream-subject.md) | a stream, through a pull consumer | [`durable`](./durable.md), [`filter_subject`](./filter-subject.md), [`deliver_policy`](./deliver-policy.md), [`max_ack_pending`](./max-ack-pending.md), [`ack_wait`](../settlement/ack-wait.md), [`pull_expires`](./batches.md) |

A setting of the other model is not a method of the descriptor, so naming one does not compile.
A descriptor goes where the subject goes, as the attribute's first argument.
The manual form passes the subject, or the descriptor, to [`subscriber(source, body)`](api:ruststream::runtime::subscriber).
The subscription's name is the subject the descriptor names: a test reads its deliveries under that name.
All subscriptions of a service share the one connection of its [`NatsBroker`](api:ruststream_nats::NatsBroker).

## Example

The subject `orders.created`, read over Core NATS.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
