---
sidebar_position: 4
title: Replies
description: "A reply answered on the request's inbox reports the reply-to header as where its address is read, and its channel carries no address."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies

A reply answered on the request's inbox reports the `reply-to` header as where its address is read, and its channel carries no address.

A NATS request carries the inbox its answer goes to, and this crate hands it to the handler as the `reply-to` header.
A reply that a transform sends to that inbox has a different address for every request.
So the operation's `reply` reports the runtime expression `$message.header#/reply-to` as its `address.location`, and the reply channel's `address` is `null`.
The channel keeps the mount site's subject as its name: the fallback for a request that named no inbox.

A reply to a fixed subject keeps that subject as its channel's address and reports no location.
A reply published with [`JetStreamPublish`](../publishing/jetstream-publish/index.md) reports none either, because a JetStream delivery's reply field is its acknowledgement inbox; a request is answered over Core NATS with `Publish`.
The [Request-reply](../request-reply/index.md) pages show both sides of a request.

## Example

The transform answers on the inbox of each request, and the app mounts it with `.out_reply(Publish).transform(ReplyTo)`.
The operation `receive_orders_quote` reports `$message.header#/reply-to`, and the channel `orders.quotes` reports no address.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_asyncapi.rs#replies"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_asyncapi.rs#replies"
```

</TabItem>
</Tabs>
