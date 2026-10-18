---
sidebar_position: 3
title: queue_group
description: "queue_group puts a Core NATS subscription in a queue group: each message goes to one member, and one name is one group across subjects."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# queue_group

[`queue_group`](api:ruststream_nats::CoreSubject::queue_group) puts a Core NATS subscription in a queue group: each message goes to one member, and one name is one group across subjects.

Without a group, every subscription on a subject receives every message.
Replicas of a service that subscribe in one group share the messages instead, and the server picks one member for each.
A subscription outside the group still receives everything, so an audit can read beside the workers.
The server keys a group by its name alone, so subscriptions with one group name on two subjects or patterns are one group.
A message both of them match is delivered once, to one member.
[`CoreSubject::queue_group`](api:ruststream_nats::CoreSubject::queue_group) and [`CoreWildcard::queue_group`](api:ruststream_nats::CoreWildcard::queue_group) take the same name.
Core NATS keeps nothing for a group: a message published while no member is subscribed is lost.
On a stream the same sharing comes from a [durable consumer](./durable.md) that several subscriptions read through.

## Example

Every replica fulfils orders in the group `fulfilment`, and the audit reads every order outside it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_queue.rs#group"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_queue.rs#group"
```

</TabItem>
</Tabs>
