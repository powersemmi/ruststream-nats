---
title: Lanes
sidebar_position: 5
description: "One subscription hands its messages over one at a time; workers(n, by_key) spreads them over lanes keyed by the nats-partition-key header."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: one stream of messages and two lanes keyed by the partition-key header",
  "text": "The diagram as text",
  "subscription": ["The subscription", "a Core subject or a pull consumer"],
  "stream": ["NatsSubscriber stream", "one message at a time"],
  "keyed": ["workers(2, by_key)", "lane = hash of nats-partition-key"],
  "lane0": ["Lane 1", "key a, in order"],
  "lane1": ["Lane 2", "key b, in order"],
  "noKey": ["A message without the header has no key:", "such messages rotate over the lanes"],
  "scale": ["Across processes: a queue group or a shared durable", "hands each message to one member"],
  "delivered": "delivered",
  "oneAtATime": "one at a time"
};

# Lanes

One subscription hands its messages over one at a time; `workers(n, by_key)` spreads them over
lanes keyed by the `nats-partition-key` header.

<Diagram name="lanes" words={words} />

Without `workers`, one loop runs the handler for each message in turn. The subscription's order is
kept, and nothing runs side by side.

`workers(n, by_key)` runs `n` lanes. A message goes to the lane its key hashes to, and a lane runs
its messages in order. NATS has no partition of its own, so the key is the
[`nats-partition-key`](../components/subscriptions/keys.md) header, which the sender sets. Messages
of one key keep their order, and different keys run side by side.

Across processes the server shares the work. A [queue group](../components/subscriptions/queue-groups.md)
hands each Core message to one member, in the order the server picks. Subscriptions that bind one
[durable](../components/subscriptions/durable.md) consumer take each JetStream message in turn.
[`max_ack_pending`](../components/subscriptions/max-ack-pending.md) bounds how many messages of a
consumer wait for an answer at once, across all its members.

A [batch handler](../components/subscriptions/batches.md) takes up to the size the mount site
names. On JetStream one batch is one fetch, closed early by `pull_expires`. On Core the batch is
assembled on the client and closes at the size or 10 milliseconds after its first message.
