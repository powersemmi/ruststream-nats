---
sidebar_position: 3
title: retry_after
description: "retry_after on JetStream puts the delay in the negative acknowledgement; on Core the framework publishes a copy after the delay."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) on JetStream puts the delay in the negative acknowledgement; on Core the framework publishes a copy after the delay.

A JetStream consumer takes the delay as part of `-NAK`.
The server holds the message for that long and delivers it again on the same consumer.
Nothing is published, so the message keeps its stream sequence, and its delivery count rises by one.
On a durable consumer the delayed message survives a restart of the service, because the server holds it.

A Core subject has no acknowledgement to carry a delay.
The framework waits out the delay in the service's memory and publishes a copy of the message, payload and headers, back to the subject the subscription reads.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER), raised by one on every copy.
A process that exits before the delay is over loses the copy.
A [`CoreWildcard`](../subscriptions/core-wildcard.md) reads many subjects, so its mount site names where the copy goes with [`out_retry`](./out-retry.md).
A [cap](./retries.md) declared at the mount site ends the retries of one message.

## Example

A payment the ledger has not caught up with is checked again after 30 seconds, on a Core subject and on a stream.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_retry.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_retry.rs#retry_after"
```

</TabItem>
</Tabs>
