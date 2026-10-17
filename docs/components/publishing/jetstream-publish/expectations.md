---
sidebar_position: 4
title: Expectations
description: "The expect steps make the stream refuse a publish unless it, or the message's subject, is still at the position the writer last saw."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Expectations

The expect steps make the stream refuse a publish unless it, or the message's subject, is still at the position the writer last saw.

They are publish steps of [`JetStreamPublishSteps`](api:ruststream_nats::JetStreamPublishSteps), written into the call's [`JetStreamOptions`](api:ruststream_nats::JetStreamOptions):

| Step | Header | The stream stores the message only if |
| --- | --- | --- |
| `expect_last_sequence(n)` | `Nats-Expected-Last-Sequence` | its last sequence is `n` |
| `expect_last_subject_sequence(n)` | `Nats-Expected-Last-Subject-Sequence` | the last sequence on the message's subject is `n` |
| `expect_last_message_id(id)` | `Nats-Expected-Last-Msg-Id` | the `Nats-Msg-Id` of its last message is `id` |

A refused publish fails with [`NatsError::JetStream`](api:ruststream_nats::NatsError::JetStream), and nothing is stored.
This is optimistic concurrency: a writer that read the stream at some position writes only if nobody wrote after it.
A handler reading a stream knows where its delivery sits through the [`StreamSequence`](../../context/index.md) key, so a write to the subject it read can expect that sequence.
The publish error wraps the client's error, so a handler that treats a refusal apart from a failed send finds [`NatsError::JetStream`](api:ruststream_nats::NatsError::JetStream) in its source chain.
As with [`message_id`](./message-id.md), the body imports this crate's prelude and bounds its slot on `JetStreamOptions`.

## Example

A reservation is confirmed once its cancellation window has passed, and only if nothing was written to the item's subject since; a cancellation in the window makes the stream refuse the confirmation.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_publishing_expectations.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_publishing_expectations.rs#handler"
```

</TabItem>
</Tabs>
