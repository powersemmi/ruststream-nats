---
title: Streams
sidebar_position: 1
description: "A JetStream stream stores what is published to its subjects under a sequence number, acknowledges each publish, and is read by consumers at their own positions."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A publish stored in stream ORDERS under sequence 6 and acknowledged, read by a durable consumer of two members and by an ephemeral consumer",
  "text": "The diagram as text",
  "publisher": ["Publisher", "orders.created,", "Nats-Msg-Id"],
  "ack": ["PubAck", "stream ORDERS,", "sequence 6, not a duplicate"],
  "stream": "Stream ORDERS",
  "dedup": ["A Nats-Msg-Id seen in the last two minutes", "is acknowledged again and stored once"],
  "durable": "Durable consumer billing",
  "replica1": "replica 1",
  "replica2": "replica 2",
  "ephemeral": ["Ephemeral", "consumer", "removed when", "inactive"],
  "positions": ["Each consumer keeps its own position in the stream;", "the members of a durable share one"],
  "publish": "publish",
  "acknowledged": "PubAck",
  "reads": "reads"
};

# Streams

A JetStream stream stores what is published to its subjects under a sequence number, acknowledges
each publish, and is read by consumers at their own positions.

<Diagram name="streams" words={words} />

A stream captures the messages published to the subjects it lists, here `orders.*`. It numbers
them in order and keeps them until its retention policy removes them: by limits of count, size and
age, while some consumer still has interest, or until a consumer of a work queue acknowledges them.

The publisher gets an answer for every message: the stream's name, the sequence and whether the
message was a duplicate. A publish no stream captures gets an error instead of the silence of Core
NATS.

The headers of a message state what the server checks before storing it. `Nats-Msg-Id` names the
message, and a repeat inside the stream's duplicate window (two minutes by default) is acknowledged
as a duplicate and stored once. `Nats-Expected-Stream`, `Nats-Expected-Last-Sequence`,
`Nats-Expected-Last-Subject-Sequence` and `Nats-Expected-Last-Msg-Id` make the server refuse the
publish when the stream is not in the state the publisher expects.

`ruststream-nats` creates no stream. A stream is created and configured with the `nats` CLI or the
JetStream API, and a subscription finds it by name when the service starts.

The model is the NATS server's own
([streams](https://docs.nats.io/nats-concepts/jetstream/streams),
[headers](https://docs.nats.io/nats-concepts/jetstream/headers),
[deduplication](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive#message-deduplication)).
How a consumer reads a stream is on [Consumers](consumers.md).
