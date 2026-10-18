---
sidebar_position: 9
description: "Connect a NATS service to a production cluster: several servers, a .creds file, TLS with the cluster's certificate authority, and a test of the configuration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to production

A production cluster has several servers, asks the service who it is, and speaks TLS.
Every one of these is a setting of the async-nats [`ConnectOptions`](../components/broker/connect-options.md), which the broker takes before anything is dialled.

## Servers, credentials and TLS

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
async-nats = "0.50"
```

The deployment provides the servers and two file paths in environment variables.
[`NatsBroker::new`](../components/broker/index.md) takes one address or a comma-separated list, and the client picks one of them to connect to.
[`with_options`](api:ruststream_nats::NatsBroker::with_options) attaches the `ConnectOptions` and still dials nothing.

A `.creds` file holds a user JWT and its NKey seed, which is how Synadia Cloud and the accounts set up with `nsc` identify a service.
The app reads the file and hands its contents to `ConnectOptions::with_credentials`, which parses them at once.
`ConnectOptions::with_credentials_file` reads the file itself, but it is `async`, and the app is built in a synchronous function.
A file that does not parse stops the service before it connects.
A server set up with a user and a password takes [`ConnectOptions::with_user_and_password`](../components/broker/security.md), with a token `with_token`, and with an NKey alone `with_nkey`.

`add_root_certificates` names the certificate authority that signed the servers' certificates, and `require_tls` refuses a connection without TLS.
`add_client_certificate` adds the service's own certificate where the cluster asks for mutual TLS.
The client reads certificate files when it connects.
`name` shows the service in the server's monitoring, and `connection_timeout` bounds each attempt to connect.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`app` builds the broker from the environment and hands it to `service`, the same function the tests call.
The runtime connects at startup.
After that the client reconnects on its own when a server goes away and restores the subscriptions.
The [generated AsyncAPI document](../components/asyncapi/server.md) reports each server's host and port, never the credentials.

A client built outside the framework, such as one shared with other code of the process, becomes the connected broker with [`ConnectedNatsBroker::from_client`](api:ruststream_nats::ConnectedNatsBroker::from_client).
Only the plain `NatsBroker` fits the synchronous app function, so `with_options` is the way wherever it can express the setting.

## Testing the configuration

The tests start the production `service` with the production `broker`, given test credentials, under `TestApp::start`.
The [in-process NATS](../components/testing/index.md) parses the addresses as the client does and dials nothing, so the credentials and the certificate path never reach a server.
A second test hands `broker` a `.creds` file that does not parse and expects the error.
Authentication and TLS themselves happen only on a real cluster.
