---
sidebar_position: 3
title: Operations
description: "A receive operation reports its queue group in the nats binding, the one field that binding has."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A receive operation reports its queue group in the `nats` binding, the one field that binding has.

The binding is `{"queue": "<group>", "bindingVersion": "0.1.0"}`.
The group is the one [`queue_group`](../subscriptions/queue-group.md) names on a [`CoreSubject`](../subscriptions/core-subject.md) or a [`CoreWildcard`](../subscriptions/core-wildcard.md).
A subscription outside a queue group carries no binding.
A JetStream consumer carries none either: its durable name and its other settings are on the [channel](./channels.md).

## Example

The operation `receive_orders_created` reports the queue group `workers`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
