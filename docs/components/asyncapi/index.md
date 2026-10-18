---
title: NATS bindings
description: "With asyncapi the generated document speaks NATS: the server, the queue group of each operation, the JetStream settings of each channel."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# NATS bindings

With the `asyncapi` feature the generated document speaks NATS: the server, the queue group of each operation, the JetStream settings of each channel.

The core builds the AsyncAPI document from the app, before anything connects.
This crate fills in what only NATS knows.
The [server](./server.md) is the host and port of each configured address.
A receive [operation](./operations.md) reports its queue group in the specification's `nats` binding, version `0.1.0`.
A [channel](./channels.md) read by a JetStream consumer, or written by a publisher that requires a stream, reports those settings under `x-ruststream-jetstream`.
A [reply](./replies.md) answered on the request's inbox reports the header its address travels in.

The specification's `nats` binding has one field, the queue group, and JetStream has no binding of its own.
So the JetStream settings travel under an extension key, which carries no `bindingVersion`.
A subscription or a publisher with nothing to report adds nothing, not an empty object.

Everything comes from the service's own declarations: the broker, the descriptors, the publish policies and the destinations the mount sites resolved.
The document is published and shared, so no part of it carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-nats = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `nats`, the key of its server in the document.
`cargo run --example nats_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_asyncapi.rs#app"
```

</TabItem>
</Tabs>
