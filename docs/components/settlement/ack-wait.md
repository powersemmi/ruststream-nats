---
sidebar_position: 2
title: ack_wait
description: "ack_wait is how long the server waits for a settlement; a delivery left unsettled comes back after it and counts as an attempt."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ack_wait

[`ack_wait`](api:ruststream_nats::JetStreamSubject::ack_wait) is how long the server waits for a settlement; a delivery left unsettled comes back after it and counts as an attempt.

It is a setting of the consumer, so [`JetStreamSubject`](../subscriptions/jetstream-subject.md) carries it, and the server keeps it with the consumer it creates at startup.
The default is 30 seconds.
The window opens when the server hands out a delivery and closes when the handler's outcome reaches it as an acknowledgement.
A delivery still unsettled at the end of the window is delivered again: the handler took longer than the window, or the process stopped before it settled.
The handler then sees the same message a second time, with its delivery count raised, while the first run may still be going on.
So the window is set longer than the slowest run of the handler.
A redelivery after the window counts toward [`max_attempts`](./retries.md), because the count is the server's own.
The value is a [`NonZeroDuration`](api:ruststream_nats::NonZeroDuration): a literal written with `nonzero!` is checked while the crate compiles, so a window of zero does not build.

## Example

Rendering a long invoice takes up to a minute, so the consumer waits two minutes for the settlement.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_settlement_ack_wait.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_settlement_ack_wait.rs#descriptor"
```

</TabItem>
</Tabs>
