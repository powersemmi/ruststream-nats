---
sidebar_position: 5
title: Prelude
description: "One glob, ruststream_nats::prelude, brings the core prelude, the broker, its descriptors and its publish policies under mount-site names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_nats::prelude`](api:ruststream_nats::prelude), brings the core prelude, the broker, its descriptors and its publish policies under mount-site names.

`use ruststream_nats::prelude::*;` is the one import a NATS service writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`NatsBroker`](./index.md), the descriptors [`CoreSubject`](api:ruststream_nats::CoreSubject), [`CoreWildcard`](api:ruststream_nats::CoreWildcard) and [`JetStreamSubject`](api:ruststream_nats::JetStreamSubject), and [`NonZeroDuration`](api:ruststream_nats::NonZeroDuration) for their time settings.

The publish policies come under the names a mount site uses on every broker.
`Publish` is [`NatsPublish`](../publishing/index.md), plain Core NATS publishing.
[`JetStreamPublish`](../publishing/jetstream-publish/index.md) keeps its name, because JetStream is the concept and not this broker's word for one.
`Publish` also makes requests, so the prelude carries the [`RequestReply`](../request-reply/index.md) capability and no separate `Request` policy.
Two broker preludes in one file both bring a `Publish`, and such a file names the prefixed type from the crate root.

A handler body bounds its slots with capability traits, such as `Out<impl Publisher>` or `Out<impl RequestReply>`, and names no broker type.
The one exception is a body that sets a per-message JetStream setting, such as a message id.
It needs [`JetStreamPublishSteps`](api:ruststream_nats::JetStreamPublishSteps) in scope and the slot bounded `Out<impl Publisher<Options = JetStreamOptions>, Marker>`, both from this prelude.
The keys of the [JetStream context](../context/index.md) live in `ruststream_nats::context::keys`.

## Example

The descriptors, their settings and the reply policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
