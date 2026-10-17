---
title: Testing
description: "TestApp::start connects NatsBroker in process: subjects, queue groups, streams, consumers and request-reply, with no server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`NatsBroker`](../broker/index.md) in process: subjects, queue groups, streams, consumers and request-reply, with no server.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<NatsBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.

The in-process mode has no settings of its own.
It parses the addresses as the client parses them, so an address the client refuses fails `TestApp::start`.
It reads the two [connection options](../broker/connect-options.md) that change what the server delivers, `no_echo` and the inbox prefix.

It routes as a server routes.
A Core NATS subscription receives every subject its subject or pattern matches, and a queue group hands each message to one of its members.
A stream stores what its consumers' filter subjects place in it, and a stream a subscription names is there for it.
A consumer starts where its deliver policy says, numbers its deliveries, and fills every key of the [JetStream context](../context/index.md).
A JetStream delivery is acknowledged, redelivered, terminated or held for a delay as on a server, and one left unsettled comes back after its `ack_wait`.
A Core NATS delivery has no acknowledgement and is never redelivered.
A request to a subject nobody reads fails at once with "no responders".

It refuses what a server refuses: a subject with whitespace, a header with no NATS form, a message over the 1 MiB payload limit, a JetStream publish no stream stores, an expectation that does not hold, and every publish after the broker shut down.
What only a server has is for a [live test](./live.md): a stream's own subject list and limits, a stream the server lacks, `max_ack_pending`, authentication and TLS, and the connect itself.

A publish through the harness returns once every handler it woke has settled, a reply included.
The assertions that follow read finished state.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

The tests publish a payment and read how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_testing.rs#testapp"
```

</TabItem>
</Tabs>

The ledger is not ready for a capture on its first delivery and asks the stream to hold it for five seconds.
The test runs on a paused clock, and `tb.advance` moves it on, so the stream delivers the capture again at once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_testing.rs#redelivery"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_testing.rs#redelivery"
```

</TabItem>
</Tabs>
