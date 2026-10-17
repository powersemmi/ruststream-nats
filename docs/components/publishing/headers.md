---
sidebar_position: 1
title: Headers
description: "A NATS header is text: a name of printable ASCII without a colon and a one-line value; a header that does not fit fails the publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

A NATS header is text: a name of printable ASCII without a colon and a one-line value; a header that does not fit fails the publish.

The framework's header map holds any bytes under any name, and NATS carries less.
A value that is not UTF-8, a value with a line break, and a name with a colon or a space have no wire form.
Such a header fails the publish with [`NatsError::Publish`](api:ruststream_nats::NatsError::Publish) naming it, before anything reaches the server.
The message is not sent without it, because a header that silently disappears changes what the message means.
A message type declares its headers in its typed header contract, `#[outgoing(headers = ..)]`, and a publish of that type that does not set them does not compile.
Headers count toward the server's payload limit together with the payload.
A delivery's headers are read with `ctx.headers()` or the [`Headers<T>`](api:ruststream::runtime::Headers) extractor.
A Core delivery that is a request carries its inbox as the `reply-to` header, which a [responder](../request-reply/responders.md) answers on.
The `Nats-` headers of [JetStream](./jetstream-publish/index.md) are written by the policy and the publish steps.

## Example

The tenant of an invoice travels as a header; a tenant name with a line break fails the publish, and the order is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_publishing_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_publishing_headers.rs#headers"
```

</TabItem>
</Tabs>
