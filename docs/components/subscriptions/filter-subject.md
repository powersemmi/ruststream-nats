---
sidebar_position: 6
title: filter_subject
description: "filter_subject narrows the subjects of the stream a consumer reads; it defaults to the subject the descriptor names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# filter_subject

[`filter_subject`](api:ruststream_nats::JetStreamSubject::filter_subject) narrows the subjects of the stream a consumer reads; it defaults to the subject the descriptor names.

A stream often stores several kinds of message under one prefix, such as every order event under `orders.>`.
The consumer's filter decides which of them it is handed; the server skips the rest for this consumer.
Without `filter_subject` the filter is the subject given to `JetStreamSubject::new`.
With it, the descriptor's subject stays the subscription's name, and the filter decides what arrives.
A filter may be a pattern with `*` and `>`.
A durable consumer has one filter, the one its last subscription opened with.

## Example

The stream `ORDERS` stores every order event, and the refunds consumer reads `orders.refunded` only.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_filter.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_filter.rs#descriptor"
```

</TabItem>
</Tabs>
