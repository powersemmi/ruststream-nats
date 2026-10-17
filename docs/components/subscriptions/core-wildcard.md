---
sidebar_position: 2
title: CoreWildcard
description: "CoreWildcard reads every Core NATS subject a pattern matches, * for one token and > for the rest, and the mount site names where a delayed copy goes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# CoreWildcard

[`CoreWildcard`](api:ruststream_nats::CoreWildcard) reads every Core NATS subject a pattern matches, `*` for one token and `>` for the rest, and the mount site names where a delayed copy goes.

A subject is a list of tokens separated by dots.
`orders.*` matches `orders.created` and `orders.cancelled`, but not `orders.eu.created`.
`orders.>` matches all three, and `>` alone matches every subject.
[`CoreWildcard::new`](api:ruststream_nats::CoreWildcard::new) takes the pattern, and its one setting is the [queue group](./queue-group.md).
The subscription's name is the pattern itself.
A pattern names many subjects and none of them in particular, so the crate does not choose where a delayed retry copy goes.
The mount site names that subject with [`out_retry(Publish).to(subject)`](api:ruststream::runtime::Mounting::out_retry).
A registration over a pattern that names none stops the service at startup, with an error naming the subscription.
A subject the pattern matches brings the copy back to the same handler.

## Example

Every one-token order event is audited, and a delayed copy goes to `orders.audit`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_subscriptions_wildcard.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_subscriptions_wildcard.rs#descriptor"
```

</TabItem>
</Tabs>
