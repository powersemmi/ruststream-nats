---
title: RequestReply
description: "request publishes with a reply inbox and resolves with the first answer, or with NatsError::RequestTimeout when nothing answers in time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RequestReply

[`request`](api:ruststream::RequestReply::request) publishes with a reply inbox and resolves with the first answer, or with [`NatsError::RequestTimeout`](api:ruststream_nats::NatsError::RequestTimeout) when nothing answers in time.

NATS correlates an answer with its request itself, so the Core publisher, [`NatsPublisher`](api:ruststream_nats::NatsPublisher), implements [`RequestReply`](api:ruststream::RequestReply).
A handler bounds its slot `Out<impl RequestReply, Slot>`, and the mount site binds the plain `Publish` policy to it.
The client subscribes to an inbox of its own and publishes the request with that inbox as the reply subject.
The first message on the inbox is the answer, and the call returns it as the delivery it is; the handler decodes the payload.
The request is bytes the handler encoded, and it goes to the connection directly, past the slot's transforms.
When nothing subscribes to the subject, the server says so at once, and the call fails with [`NatsError::Publish`](api:ruststream_nats::NatsError::Publish) instead of waiting out the timeout.
A request is Core NATS: it reaches a [responder](./responders.md) listening at that moment, and nothing stores it.
The same slot also publishes with the builder, what its `#[publishes(..)]` list admits.

## Example

The handler asks the price service for the price of an order's item, then publishes the priced order through the same slot.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_request_reply.rs#request"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_request_reply.rs#request"
```

</TabItem>
</Tabs>

The mount binds `Publish` to the slot; the price service runs in the same app here.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_request_reply.rs#app"
```

</TabItem>
</Tabs>
