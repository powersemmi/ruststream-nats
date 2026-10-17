---
sidebar_position: 5
title: durable
description: "durable names a JetStream consumer: the server keeps its position across restarts, and subscriptions sharing it take messages in turn."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# durable

[`durable`](api:ruststream_nats::JetStreamSubject::durable) names a JetStream consumer: the server keeps its position across restarts, and subscriptions sharing it take messages in turn.

A named consumer lives on the server, apart from any subscription.
It remembers which messages were acknowledged, so a restarted service resumes after the last one and reads what arrived while it was down.
A consumer without a name is ephemeral and starts from its [deliver policy](./deliver-policy.md) on every startup.
Every replica of a service that reads through one durable shares its messages: each message goes to one of them, as in a [queue group](./queue-group.md).
At startup the subscription creates the consumer, or updates it when it exists.
So the settings the last subscription opened with are the consumer's settings, and replicas that share a durable name one filter and one policy.

## Example

Every replica bills through the consumer `billing`, and a restarted service resumes where the consumer stopped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_durable.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_durable.rs#descriptor"
```

</TabItem>
</Tabs>
