---
sidebar_position: 4
title: Shutdown
description: "Shutdown waits for the handlers in flight and drains the connection; a publisher used afterwards reports NatsError::Closed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown waits for the handlers in flight and drains the connection; a publisher used afterwards reports [`NatsError::Closed`](api:ruststream_nats::NatsError::Closed).

The runtime stops reading first and waits for the handlers that are still running.
The app's `shutdown_timeout` bounds that wait; with none set it waits for every handler.
Then the broker drains the connection: the client ends its subscriptions, sends what it still buffers and closes.
So a reply or a publish a handler made before the shutdown reaches the server before the service exits.
A drain that fails ends the shutdown with [`NatsError::Shutdown`](api:ruststream_nats::NatsError::Shutdown).

The broker marks the connection closed before it drains.
A publisher taken earlier keeps pointing at the connection, and a publish through it returns `NatsError::Closed` with the subject it was asked to reach.
It never sends into a connection that is going away.

A tool that drives the broker by hand receives a [`ClosedNatsBroker`](api:ruststream_nats::ClosedNatsBroker) from the shutdown.
It carries what the connection did over its life: [`messages_sent`](api:ruststream_nats::ClosedNatsBroker::messages_sent), [`messages_received`](api:ruststream_nats::ClosedNatsBroker::messages_received) and [`connects`](api:ruststream_nats::ClosedNatsBroker::connects), reconnects included.

## Example

Shutdown gives the handlers ten seconds, and the shipments they replied with reach the server before the service exits.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
