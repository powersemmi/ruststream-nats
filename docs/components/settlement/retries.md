---
sidebar_position: 4
title: Retries
description: "max_attempts caps the deliveries of one message; on JetStream it counts the server's own deliveries, on Core the copies in a header."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) caps the deliveries of one message; on JetStream it counts the server's own deliveries, on Core the copies in a header.

It is declared at the mount site, and the first delivery counts as one.
A JetStream consumer reports how many times it has delivered a message, so the cap reads that number.
Every delivery counts: a `retry`, a [`retry_after`](./retry-after.md), and a redelivery after [`ack_wait`](./ack-wait.md).
A Core subject delivers each message once, so the framework counts the copies it publishes, in [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER).
Under a cap every retry on a Core subject travels as such a copy, an immediate one too.
The delivery that reaches the cap goes to the [dead-letter subject](./dead-letters.md), or is terminated when the mount site names none.
The cap is the framework's own: the consumer the crate creates leaves `max_deliver` at the server's default, unlimited, so the server delivers a retried message again until the cap ends it.

## Example

Five deliveries of one payment, on a Core subject and on a stream; the fifth that still fails goes to `payments.dead`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_retry.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_retry.rs#declaration"
```

</TabItem>
</Tabs>
