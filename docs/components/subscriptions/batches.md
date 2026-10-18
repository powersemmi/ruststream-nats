---
sidebar_position: 9
title: Batches
description: "A slice handler takes a batch: one JetStream pull fetch of the mount site's size, or a Core NATS batch the client assembles."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler takes a batch: one JetStream pull fetch of the mount site's size, or a Core NATS batch the client assembles.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
On a [`JetStreamSubject`](./jetstream-subject.md) one batch is one pull request: the consumer asks the server for up to `n` messages.
The fetch ends once it holds `n`, or once [`pull_expires`](api:ruststream_nats::JetStreamSubject::pull_expires) runs out with fewer, 5 seconds by default.
`pull_expires` takes a [`NonZeroDuration`](api:ruststream_nats::NonZeroDuration), because a fetch that expires at once would spin.
Core NATS has no batch on the wire, so the client collects up to `n` deliveries and closes a short batch 10 ms after its first one.
One outcome settles every message of the batch.
A `Vec<HandlerOutcome>` settles message `i` with outcome `i`.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acknowledges the batch and `Err(outcomes)` settles message by message.

## Example

Three orders waiting in the stream arrive in one call.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_batches.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_batches.rs#batch"
```

</TabItem>
</Tabs>
