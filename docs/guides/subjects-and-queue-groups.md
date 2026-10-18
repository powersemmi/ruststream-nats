---
sidebar_position: 2
description: "Name Core NATS subjects, read many with a wildcard, share the work in a queue group and keep the order of each key in parallel lanes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subjects, wildcards and queue groups

This guide names Core NATS subjects, reads many of them with a wildcard, shares the work between replicas in a queue group and keeps the order of each key in parallel lanes.

A subject is a name of tokens separated by dots, such as `orders.eu.created`.
Core NATS hands a message to every subscription whose subject matches at that moment, and stores nothing.

The example is an orders service.
It packs the European orders, bills them, and audits every order event of every region.

## A queue group

`pack` reads `orders.eu.created`.
The service runs in several replicas, and each order must be packed once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#pack"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#pack"
```

</TabItem>
</Tabs>

[`CoreSubject`](../components/subscriptions/core-subject.md) reads one subject.
[`queue_group`](../components/subscriptions/queue-group.md) puts the subscription into the group `packers`.
The server hands each message to one member of a group, so each replica packs a share of the orders.
A group name is one group for the server, even across different subjects.
A subscription outside the group, or in another group, still gets its own copy.

## Parallel lanes per customer

`bill` reads the same subject without a group, so it gets every order that `pack` gets.
It bills up to eight orders at once and keeps the order of each customer.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#bill"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#bill"
```

</TabItem>
</Tabs>

`workers(8, by_key)` runs eight lanes.
Deliveries with the same [key](../components/subscriptions/keys.md) run one after another on one lane.
In the manual form the same setting is [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).
NATS has no native key, so the key travels in the `nats-partition-key` header ([`PARTITION_KEY_HEADER`](api:ruststream_nats::PARTITION_KEY_HEADER)), and the sender sets it.
`CustomerKey` is a header contract with one field under that name.
`Headers<CustomerKey>` reads the key of the delivery, and `with_headers(&key)` writes it on the invoice.
So the invoices of one customer keep their order for the next reader too.
An order without the header fails the contract and is settled by the decode policy, which drops it by default.

A NATS header is text: the name is printable ASCII without a colon, and the value is one line.
A header that does not fit fails the publish.

## A wildcard

`audit` reads every order event of every region.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#audit"
```

</TabItem>
</Tabs>

[`CoreWildcard`](../components/subscriptions/core-wildcard.md) reads every subject a pattern matches.
`*` matches one token: `orders.*.created` reads the creations of every region.
`>` matches one or more trailing tokens: `orders.>` reads every subject under `orders`.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#app"
```

</TabItem>
</Tabs>

A pattern reads many subjects, so a delayed copy of its delivery has no single subject to go to.
The mount site names one with [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md), here `orders.retry`, which the pattern also reads.
Every registration over a pattern names it, even when its handler never retries, and the service refuses to start without it.
[Retries and dead letters](retries.md) explains the copies.

## At most once

Core NATS delivers a message to the subscriptions that exist when it arrives.
A message published while no subscription matches is gone.
A delivery has no acknowledgement either, so a handler that fails does not get the message again from the server.
When every message counts, the subject belongs to a stream: [Streams and consumers](streams-and-consumers.md) shows how to read one.

The tests in the example file run the app in process and check each subscription's copy, the pattern, and the order of the invoices per customer.
