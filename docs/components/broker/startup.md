---
sidebar_position: 3
title: Startup
description: "At startup the runtime dials the servers within connection_timeout, then opens each subscription; a JetStream one finds its stream or fails the start."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

At startup the runtime dials the servers within `connection_timeout`, then opens each subscription; a JetStream one finds its stream or fails the start.

The connect is one dial of the `async-nats` client.
It covers the TCP connection, the TLS handshake and the login, and waits up to the [`connection_timeout`](./connect-options.md) of the options, five seconds by default.
A server that refuses the connection or does not answer in time fails startup with [`NatsError::Connect`](api:ruststream_nats::NatsError::Connect), and the service exits before it reads anything.

A Core NATS subscription is open once the server has confirmed it.
The broker sends the subscription and waits for one round trip to the server, so everything published after startup reaches it.

A JetStream subscription looks up the stream its [`JetStreamSubject`](api:ruststream_nats::JetStreamSubject) names and creates its consumer there.
A durable consumer that already exists takes the descriptor's settings.
The stream itself belongs to the server: it is created before the service starts, with `nats stream add` or through [`jetstream`](api:ruststream_nats::ConnectedNatsBroker::jetstream).
A stream the server lacks fails startup with [`NatsError::JetStream`](api:ruststream_nats::NatsError::JetStream).

In process, a [`TestApp`](../testing/index.md) test finds every stream a subscription names, so the missing stream and the connect timing are for a [live test](../testing/live.md).

## Example

The connect gives up after three seconds.
The tests in the same file start the service against a server: once before its stream exists, and once after.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_broker_startup.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_broker_startup.rs#broker"
```

</TabItem>
</Tabs>
