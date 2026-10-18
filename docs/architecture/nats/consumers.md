---
title: Consumers
sidebar_position: 2
description: "A JetStream pull consumer hands out batches the client asks for, and waits for an answer to each message: an acknowledgement, a negative one, or a termination."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A pull consumer of stream ORDERS: the client asks for a batch, receives messages and answers each with +ACK, -NAK or +TERM",
  "text": "The diagram as text",
  "stream": "Stream ORDERS",
  "consumer": ["Pull consumer billing", "a position in the stream, a filter subject,", "a deliver policy, ack_wait, max_ack_pending"],
  "client": ["The subscription", "takes one message at a time"],
  "ack": ["+ACK", "done: never", "delivered again"],
  "nak": ["-NAK", "delivered again,", "now or after", "a delay"],
  "term": ["+TERM", "ended: never", "delivered again"],
  "redelivery": ["A message left unanswered past ack_wait is delivered again,", "and its delivered count grows by one"],
  "messages": "messages",
  "pull": "pull: batch, expiry"
};

# Consumers

A JetStream pull consumer hands out batches the client asks for, and waits for an answer to each
message: an acknowledgement, a negative one, or a termination.

<Diagram name="consumers" words={words} />

A consumer is a view of one stream with a position of its own. Its filter subject narrows which of
the stream's messages it reads, and its deliver policy says where a new consumer starts: the first
message, the last, new ones only, a sequence or a time. A durable consumer has a name, survives a
restart, and is shared by every client that binds to that name. An ephemeral consumer has none,
and the server removes it once nobody reads it.

A pull consumer sends nothing until the client asks. A pull request names a batch size and an
expiry, and the server answers with up to that many messages before the expiry. `async-nats` keeps
the requests going: it asks for 200 messages at a time, asks again when half of them are taken,
and expects a heartbeat every 15 seconds while it waits.

Every message waits for an answer. `+ACK` finishes it. `-NAK` sends it again, at once or after the
delay it names. `+TERM` ends it without another delivery. A message with no answer within
`ack_wait` is delivered again, and each delivery raises its `delivered` count. `max_ack_pending`
caps how many messages may wait for an answer at once: at the cap the consumer hands out no more.

The acknowledgement subject of each delivery carries its metadata: the stream and consumer names,
the stream and consumer sequences, the `delivered` count and the number of messages still pending.

`ruststream-nats` creates the consumer when the service starts and sends `ack_wait` (30 seconds),
`max_ack_pending` (1024) and the deliver policy (all messages) explicitly; the rest of the
configuration keeps the server's defaults.

The model is the NATS server's own
([consumers](https://docs.nats.io/nats-concepts/jetstream/consumers),
[acknowledgement models](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#acknowledgement-models)).
