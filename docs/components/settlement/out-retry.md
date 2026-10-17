---
sidebar_position: 6
title: out_retry
description: "A pattern names no subject of its own, so out_retry(Publish).to(subject) says where its delayed copies go; without it the app refuses to start."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

A pattern names no subject of its own, so [`out_retry(Publish).to(subject)`](api:ruststream::runtime::Mounting::out_retry) says where its delayed copies go; without it the app refuses to start.

A [`CoreWildcard`](api:ruststream_nats::CoreWildcard) reads every subject its pattern matches, and a copy has to go to one of them.
This crate does not take the pattern itself as a destination, so the mount site names the subject.
A registration over a pattern that names no subject fails the start with an error naming the subscription.
The service reports the gap at startup instead of losing every delayed message once it runs.
`out_retry` binds the [`Retry`](api:ruststream::runtime::Retry) slot, the one the framework publishes its copies through, to a policy; `Publish` is plain Core NATS.
`.to(subject)` names the subject; a publish transform that declares `Names` can choose one per delivery instead.
A subject the pattern matches brings the copy back to the same handler.
The [dead-letter subject](./dead-letters.md) stays outside the pattern, or the spent message comes back too.
A [`CoreSubject`](api:ruststream_nats::CoreSubject) and a [`JetStreamSubject`](api:ruststream_nats::JetStreamSubject) name their own destination, so they need no `out_retry`; on them the slot only changes the policy or adds a transform.

## Example

Entries of every region are audited again a minute later through `audits.retry`, which the pattern reads.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_settlement_out_retry.rs#out_retry"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_settlement_out_retry.rs#out_retry"
```

</TabItem>
</Tabs>
