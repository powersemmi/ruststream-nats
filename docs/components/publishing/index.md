---
title: NatsPublish
description: "NatsPublish, Publish in the prelude, is plain Core NATS publishing: a publish is handed to the connection and does not wait for the server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# NatsPublish

[`NatsPublish`](api:ruststream_nats::NatsPublish), `Publish` in the prelude, is plain Core NATS publishing: a publish is handed to the connection and does not wait for the server.

A mount site names it wherever a registration publishes: `.out_reply(Publish)` for a reply, `.out(Slot, Publish)` for a slot, `.out_retry(Publish)` for [retry copies](../settlement/out-retry.md), and `b.after_startup(Publish, ..)` for a publish outside any delivery.
The policy holds no connection; the runtime pairs it with the connected broker at startup into a [`NatsPublisher`](api:ruststream_nats::NatsPublisher).
It is the broker's default policy, so a replying handler mounted without `.out_reply(..)` replies through it.
A publish returns once the message is queued on the connection.
The server delivers it to the subscribers connected at that moment, and a subject nobody reads drops it without an error.
A publish fails when the client refuses the message: a header with no NATS form, a payload over the server's limit, or a connection the broker already shut down, which reports [`NatsError::Closed`](api:ruststream_nats::NatsError::Closed).
Core NATS has no per-message setting, so the publisher's options type is `()`.
The same live publisher serves [requests](../request-reply/index.md).
A message that has to be stored and acknowledged goes through [`JetStreamPublish`](./jetstream-publish/index.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_publishing_publish.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_publishing_publish.rs#publish"
```

</TabItem>
</Tabs>
