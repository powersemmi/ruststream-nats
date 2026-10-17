---
sidebar_position: 1
title: Server
description: "The server reports the host and port of each configured address, never credentials; the protocol version is read once connected."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host and port of each configured address, never credentials; the protocol version is read once connected.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the host and port of every address [`NatsBroker::new`](../broker/index.md) took, joined with commas.
The scheme and any user, password or token an address carries stay out of it.
The `protocol` is `nats`.

The [credentials and TLS settings](../broker/security.md) of the connect options never reach the document either.
The version of the NATS client protocol is a fact of the live connection, and the document is built before anything dials, so `protocolVersion` stays out.
[`ConnectedNatsBroker::server_spec`](api:ruststream_nats::ConnectedNatsBroker::server_spec) reports it once connected, with the host and port the server announced.

## Example

The broker reaches two servers with a password in each address.
Its server in the document is `nats-1.internal:4222,nats-2.internal:4222`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_asyncapi.rs#server"
```

</TabItem>
</Tabs>
