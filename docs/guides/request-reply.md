---
sidebar_position: 7
description: "Ask another service over NATS and wait for its answer, answer requests on their inbox, handle timeouts and no responders, and scale responders."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request-reply

This guide asks another service over NATS and waits for its answer, answers requests on their inbox, handles a request nobody answers, and scales the answering side.

NATS correlates a reply with its request itself.
A request carries an inbox, a subject unique to it, and the answer is published to that inbox.

The example is two services in one app.
The pricing service answers questions about prices, and the orders service asks it for the price of each order.

## The responder

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

A request arrives with its inbox in the `reply-to` header, and `ctx.headers().reply_to()` reads it.
`quote` publishes the price through its `Answers` slot with [`to(inbox)`](../components/request-reply/responders.md), because the destination is known only per request.
`Price` therefore declares no subject of its own.
An item without a price gets no answer, and the requester's timeout ends the wait.

The responder reads `prices.quote` in the queue group `pricing`.
Each replica of the pricing service joins the group, and each request goes to one of them.
Any NATS responder answers the same request, for example `nats reply prices.quote '{"sku":"book","cents":1250}'` from the command line.

## The requester

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#requester"
```

</TabItem>
</Tabs>

`price_order` reads new orders from a stream and asks for each price on `prices.quote`.
The `Pricing` slot is bounded `Out<impl RequestReply, _>`: the body says it needs [request-reply](../components/request-reply/index.md) and names no broker.
The app pairs the slot with the plain `Publish` policy, whose publisher carries request-reply on NATS.
[`request`](api:ruststream::RequestReply::request) publishes the question with a fresh inbox and resolves with the first message on it.
The question is a raw message, so the body encodes it and decodes the answer itself, here with `serde_json`.
The order's reply, `Priced`, goes out once the price is known.

A request fails in two ways.
When no subscription reads the subject, the server answers at once with "no responders".
When a responder exists but stays silent, the request fails after its timeout, two seconds here, with [`NatsError::RequestTimeout`](api:ruststream_nats::NatsError::RequestTimeout).
Either way the order waits 30 seconds on the stream and asks again.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#app"
```

</TabItem>
</Tabs>

Both slots take the plain `Publish` policy: one publishes answers, the other asks questions.
A startup hook asks the same way, with `b.after_startup(Publish, ..)` and `request` on the publisher it is handed.

The tests in the example file run both halves in one app in process.
The in-process NATS routes a request to the app's own responder, answers "no responders" when no subscription reads the subject, and times a silent responder out.
