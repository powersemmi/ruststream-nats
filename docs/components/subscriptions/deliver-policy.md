---
sidebar_position: 7
title: DeliverPolicy
description: "DeliverPolicy says where a newly created consumer starts: all, new, last, last per subject, a sequence or a time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DeliverPolicy

[`DeliverPolicy`](api:ruststream_nats::DeliverPolicy) says where a newly created consumer starts: all, new, last, last per subject, a sequence or a time.

[`deliver_policy`](api:ruststream_nats::JetStreamSubject::deliver_policy) sets it on the descriptor.
The type is the one `async-nats` defines, re-exported by the crate.
`All`, the default, starts with the oldest message the stream holds.
`New` starts with the first message published after the consumer was created.
`Last` starts with the newest message of the stream, and `LastPerSubject` with the newest message of each subject the filter matches.
`ByStartSequence` starts at a stream sequence, and `ByStartTime` at the first message stored at or after a time.
The policy applies once, when the consumer is created.
A [durable](./durable.md) consumer that already exists resumes from its own position.
An ephemeral consumer is created on every startup, so it starts from its policy each time.

## Example

A live ticker starts with the next price, and the prices the stream held before it started stay behind.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_deliver.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_deliver.rs#descriptor"
```

</TabItem>
</Tabs>
