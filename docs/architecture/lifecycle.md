---
title: Lifecycle
sidebar_position: 3
description: "NatsBroker::new records configuration, connect dials, startup subscribes each subject or creates each consumer, and shutdown drains."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on NATS",
  "text": "The diagram as text",
  "states": "Broker",
  "nats": "On NATS",
  "unconnected": ["NatsBroker::new", "(addresses)"],
  "connected": ["ConnectedNatsBroker"],
  "closed": ["ClosedNatsBroker", "the drained counters"],
  "config": ["Addresses and", "options, no I/O"],
  "dial": ["Dial with the", "connect options"],
  "core": ["Each Core subscription:", "SUB, then a flush"],
  "jetstream": ["Each JetStream one:", "find the stream,", "create the consumer"],
  "drain": ["Mark closed,", "then drain"],
  "afterShutdown": ["A publisher that outlives the connection", "answers NatsError::Closed, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`NatsBroker::new`](../components/broker/index.md) records configuration, connect dials, startup
subscribes each subject or creates each consumer, and shutdown drains.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. It takes one address or a
comma-separated list, and [`with_options`](../components/broker/connect-options.md) attaches the
`async-nats` connect options. The runtime connects the broker when the app starts. Connect dials
with those options, and a server it cannot reach fails the start with `NatsError::Connect`.

Each subscription's [descriptor](../components/subscriptions/index.md) resolves once, against the
connected broker. A Core subscription sends `SUB` and then waits for a flush round trip. The server
has registered the subscription by then, so a producer on another connection cannot publish into a
gap. A JetStream subscription looks its stream up by name and creates its pull consumer, both
requests the server answers. A stream that does not exist fails the start with
`NatsError::JetStream`.

[Shutdown](../components/broker/shutdown.md) marks the connection closed first, so a publisher
that aliases it cannot slip a message in. Then it drains the client: the subscriptions stop, what
was already published is flushed, and the connection closes. [`ClosedNatsBroker`](api:ruststream_nats::ClosedNatsBroker)
carries the client's counters: messages sent, messages received and connects.

A publisher paired before the shutdown is a handle the shutdown cannot take back. It answers
`NatsError::Closed` with the subject it was asked to publish to.

The in-process transport follows the same ladder. It parses the addresses as connect does, and
reads `no_echo` and the inbox prefix from the connect options.
