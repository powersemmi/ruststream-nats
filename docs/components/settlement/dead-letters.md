---
sidebar_position: 5
title: Dead letters
description: "dead_letter names the subject a spent delivery is published to; a capped JetStream consumer with none terminates it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`dead_letter(subject)`](api:ruststream::runtime::Mounting::dead_letter) names the subject a spent delivery is published to; a capped JetStream consumer with none terminates it.

It follows [`max_attempts(n)`](./retries.md) at the mount site.
When a retry would make the delivery the `n`-th one, the framework settles it and publishes it to the dead-letter subject instead, payload and headers as they arrived.
On a JetStream consumer the delivery is then terminated, so the server delivers it no more.
NATS has no dead-letter mechanism of its own, so the same path serves both models, and a warning names the subscription that gave up.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER): on a Core subject it says how many deliveries the message had; on a JetStream consumer the server counted them, and the header reads 1.
A cap without a dead-letter subject terminates the spent JetStream delivery, and ends the Core one.
The dead-letter subject is an ordinary subject: a subscription of this service or of another one reads it.
It stays outside every pattern a subscription of the service reads, or the spent message arrives back at a handler.

## Example

The dead-letter subject is read by an ordinary subscription.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_retry.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_retry.rs#dead_letter"
```

</TabItem>
</Tabs>
