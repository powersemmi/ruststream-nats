---
sidebar_position: 1
title: Responders
description: "A responder reads the request's inbox from the reply-to header and answers there through an injected publisher."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Responders

A responder reads the request's inbox from the `reply-to` header and answers there through an injected publisher.

NATS carries a request's inbox in the protocol's reply field, and this crate surfaces it on a Core delivery as the `reply-to` header.
The handler reads it with `ctx.headers().reply_to()`.
The answer leaves through an `Out` slot with `.to(inbox)`, so the answer type declares no subject of its own.
The answer is an ordinary Core publish to the inbox, and the requester's client matches it to the waiting request.
A message without an inbox was not a request, and a redelivery would not add one, so the handler drops it.
A responder reads a [`CoreSubject`](api:ruststream_nats::CoreSubject), the bare name form, or a [`CoreWildcard`](api:ruststream_nats::CoreWildcard).
On a JetStream delivery the reply field is the consumer's acknowledgement subject, so no `reply-to` header is surfaced there.
Any NATS client's request reaches the responder, `nats request` included, and a [`RequestReply`](./index.md) slot of another service is one of them.
With a queue group on the subscription, each request is answered by one member of the group.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_request_reply_responder.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_request_reply_responder.rs#responder"
```

</TabItem>
</Tabs>
