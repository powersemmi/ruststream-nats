---
title: NatsBroker
description: "NatsBroker connects a service to NATS; new only records the server addresses, and the runtime connects it at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# NatsBroker

[`NatsBroker`](api:ruststream_nats::NatsBroker) connects a service to NATS; `new` only records the server addresses, and the runtime connects it at startup.

[`NatsBroker::new`](api:ruststream_nats::NatsBroker::new) takes one address or a comma-separated list: `nats://host:port`, `tls://host:port` or a bare `host:port`.
It does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
Everything else the client needs, such as its name, its reconnects, credentials and TLS, is an `async-nats` setting attached with [`with_options`](./connect-options.md).

At startup the runtime connects the broker and receives a [`ConnectedNatsBroker`](api:ruststream_nats::ConnectedNatsBroker).
The connect dials the server, so an unreachable server fails [startup](./startup.md) instead of the first publish.
Every subscription and every publisher of the service opens against that connected form.
At [shutdown](./shutdown.md) the runtime drains the connection and turns it into a [`ClosedNatsBroker`](api:ruststream_nats::ClosedNatsBroker).

`#[subscriber("orders.created")]` reads the Core NATS subject `orders.created`, outside any queue group.
The descriptors for a queue group, a subject pattern or a JetStream stream are on the [Subscriptions](../subscriptions/index.md) pages.

The crate has two features, both off by default.
`testing` adds the in-process mode a [`TestApp`](../testing/index.md) test connects, and `asyncapi` adds the [NATS bindings](../asyncapi/index.md) of the generated document.

## Example

The service reads the subject `orders.created`.
`cargo run --example nats_core --features testing -- run` starts it against a server on `localhost:4222`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_core.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_core.rs#app"
```

</TabItem>
</Tabs>
