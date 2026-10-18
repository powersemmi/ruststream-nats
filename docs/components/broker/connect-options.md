---
sidebar_position: 1
title: ConnectOptions
description: "with_options attaches async-nats ConnectOptions to the broker: the client's name, pings, reconnects, no_echo and the inbox prefix."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ConnectOptions

[`with_options`](api:ruststream_nats::NatsBroker::with_options) attaches [`async_nats::ConnectOptions`](https://docs.rs/async-nats/0.50/async_nats/struct.ConnectOptions.html) to the broker: the client's name, pings, reconnects, `no_echo` and the inbox prefix.

`ConnectOptions` is the builder of the `async-nats` client this crate runs on.
Building it does no I/O, and the broker applies it when the runtime connects.
A service that names it adds `async-nats` to its own dependencies, in the version this crate uses.
An option the service leaves unset keeps the client's default.

The options a service sets most often:

| Option | What it does | Default |
|---|---|---|
| `name` | the name the server's monitoring lists the connection under | none |
| `ping_interval` | how often the client checks that the server still answers | 60 s |
| `max_reconnects` | how many failed reconnects in a row the client makes before it gives up | no limit |
| `connection_timeout` | how long the connect at [startup](./startup.md) waits | 5 s |
| `subscription_capacity` | how many messages a subscription buffers before the client drops them as a slow consumer | 65 536 |
| `no_echo` | the connection's own publishes skip its own subscriptions | off |
| `custom_inbox_prefix` | the subject prefix of the inboxes [requests](../request-reply/index.md) are answered on | `_INBOX` |

Credentials and TLS are options too, on the [Security](./security.md) page.

`no_echo` and the inbox prefix change what the server delivers, so the in-process mode of a [`TestApp`](../testing/index.md) test reads them as the server would.
With `no_echo` a reply or a publish of the service never reaches a handler of the same service, while a message from another connection does.
The other options shape the client's own connection, which the in-process mode has none of.

## Example

The dependency line:

```toml
async-nats = "0.50"
```

The service names its connection `pricing-svc` and bounds its reconnects.
It publishes cache invalidations to a subject it also reads, and `no_echo` keeps its own invalidations from coming back to it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_broker_options.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_broker_options.rs#broker"
```

</TabItem>
</Tabs>
