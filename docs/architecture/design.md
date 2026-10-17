---
title: Design
sidebar_position: 2
description: "Where ruststream-nats sits: between the RustStream runtime and the async-nats client, with every subscription and publisher on one connection."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the async-nats client and the NATS server, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement, retries",
  "crate": "ruststream-nats",
  "subscriber": ["NatsSubscriber", "yields NatsMessage"],
  "publishers": ["NatsPublisher,", "JetStreamPublisher"],
  "context": "Context keys",
  "inProcess": "In-process transport",
  "broker": ["NatsBroker", "one connection"],
  "descriptors": ["CoreSubject,", "CoreWildcard,", "JetStreamSubject"],
  "client": "async-nats",
  "jetstream": "jetstream::Context on the client",
  "connection": "Client: one connection",
  "server": "NATS server",
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-nats` sits: between the RustStream runtime and the `async-nats` client, with every
subscription and publisher on one connection.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over
[`async-nats`](https://docs.rs/async-nats/0.50.0/async_nats/) and adds nothing to the protocol. The
runtime above it is the same for every broker. What is particular to NATS is in this crate: the
[descriptors](../components/subscriptions/index.md), [what an ack means](../components/settlement/index.md),
[where a retry copy goes](../components/settlement/retries.md) and the [metadata of a
delivery](../components/context/index.md).

A connected broker holds one client, and every publisher paired from it shares that client. A Core
subscription is one subscribe on the connection. A JetStream subscription creates one pull consumer
and reads its messages over the same connection. So a service chooses Core or JetStream per handler
and per publish, not per process.

The JetStream context is built on the runtime the broker connected on. The tasks the client starts
for it, a consumer's pull loop and the acknowledgement task, run there whichever thread asks.

A setting this crate does not type is an `async-nats` connect option, which
[`with_options`](../components/broker/connect-options.md) attaches before anything is dialled.
[`client`](api:ruststream_nats::ConnectedNatsBroker::client) and
[`jetstream`](api:ruststream_nats::ConnectedNatsBroker::jetstream) hand out the client and a
JetStream context for what the crate does not wrap, such as creating a stream.
[`from_client`](api:ruststream_nats::ConnectedNatsBroker::from_client) adopts a client built
elsewhere.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).

## The in-process transport

With the `testing` feature, [`TestApp::start`](../components/testing/index.md) connects the same
broker to an in-process transport instead of a server. The descriptors, the policies and the
deliveries are the production ones. It matches subjects with `*` and `>`, shares a queue group,
answers a request with "no responders", and keeps streams with their sequences, their duplicate
window and the expectations a publish states. Its consumers deliver from their deliver policy,
report the metadata, and settle, delay and redeliver after `ack_wait` as a server does.

What is the server's own is left to a live run: a stream's subject list and limits,
`max_ack_pending`, the pending limits of a subscription, authentication and TLS. In process a
subscription finds every stream it names; against a server, a stream that does not exist fails the
start.

Without the feature the transport is not compiled. A production build's connected broker is one
shared pointer to the connection, and the compiler checks that size.
