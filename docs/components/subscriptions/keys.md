---
sidebar_position: 10
title: Keys
description: "The nats-partition-key header keys the lanes of workers(n, by_key): messages sharing a key run on one lane, in order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Keys

The [`nats-partition-key`](api:ruststream_nats::PARTITION_KEY_HEADER) header keys the lanes of `workers(n, by_key)`: messages sharing a key run on one lane, in order.

`workers(n, by_key)` runs `n` sequential lanes in one subscription.
Messages with one key run in order on one lane, and different keys run at once.
NATS has no partitions or message keys, so the key travels in the `nats-partition-key` header.
The sender sets it: any NATS client writes the header, and a RustStream message type declares it in its typed header contract.
The value is text, because a NATS header is; a value that is not a single line of UTF-8 [fails the publish](../publishing/headers.md).
A message without the header has no key and rotates over the lanes.
The lanes work the same on Core NATS and on a stream.
The manual form writes the lanes as [`workers_by_key(n)`](api:ruststream::runtime::SubscriberSettings::workers_by_key) on the mount chain.

## Example

Two tenants' orders, each order a second of work: the tenants run side by side, two seconds in all.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_keys.rs#keys"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_keys.rs#keys"
```

</TabItem>
</Tabs>
