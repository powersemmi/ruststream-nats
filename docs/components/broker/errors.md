---
sidebar_position: 6
title: Errors
description: "NatsError is the one error type of the crate: connect, publish, subscribe, JetStream, shutdown, a request timeout and a closed connection."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`NatsError`](api:ruststream_nats::NatsError) is the one error type of the crate: connect, publish, subscribe, JetStream, shutdown, a request timeout and a closed connection.

Each variant names the step that failed.
The `async-nats` error behind it is its `source`, so the client's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the client could not parse an address or reach the server, or the server refused the login |
| `Subscribe` | the server refused a Core NATS subscription, such as one to a malformed subject |
| `JetStream` | a stream or a consumer could not be found or created, a JetStream publish was refused or not stored, or an acknowledgement failed |
| `Publish` | a publish was refused: a subject or a header the protocol cannot carry, a message over the server's payload limit, or a request nobody subscribes to |
| `RequestTimeout` | a request received no reply within its timeout |
| `Closed` | a publisher was used after the broker shut down; it carries the subject |
| `Shutdown` | draining the connection at shutdown failed |
| `InvalidOptions` | a descriptor names an empty subject, or a delayed copy is sent to a `CoreSubject` that holds a pattern |

A service meets the error in two places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A shipment over the server's 1 MiB payload limit is refused.
The handler logs the cause and settles the order as a retry.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
