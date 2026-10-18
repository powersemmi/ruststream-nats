---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a running NATS server, for what only a server has."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a running NATS server, for what only a server has.

It connects the broker through its ordinary connect, so [startup](../broker/startup.md) dials the server and every JetStream subscription looks up its stream there.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process mode leaves to the server: a stream's own subject list and limits, a stream the server lacks failing startup, `max_ack_pending`, authentication and TLS, and the timing of the connect.
A stream is the server's state, and the server keeps what earlier runs left in it.
So a test creates the stream through an admin connection's [`jetstream`](api:ruststream_nats::ConnectedNatsBroker::jetstream) before the app starts, replacing a leftover, and deletes it afterwards.

In the example, `live::url` returns the server's address from `NATS_TEST_URL`, and the test returns early without one, so `cargo test` passes on a machine with no server.
A job that stood a server up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, a NATS server with JetStream that `just brokers-up` starts.

## Example

The two tests share one body: one in process, one against the server at `NATS_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
