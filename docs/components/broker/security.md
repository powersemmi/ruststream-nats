---
sidebar_position: 2
title: Security
description: "Credentials and TLS are ConnectOptions settings: a user and password, a token, an NKey, a creds file, TLS with the service's own certificates."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Security

Credentials and TLS are [`ConnectOptions`](./connect-options.md) settings: a user and password, a token, an NKey, a creds file, TLS with the service's own certificates.

Each login has a constructor that starts the options: `with_user_and_password`, `with_token`, `with_nkey` with the seed, and `with_credentials` with the contents of a `.creds` file (a JWT and its NKey seed).
`with_credentials_file` reads the file itself and is `async`, so the synchronous app builder reads the file and passes its contents to `with_credentials` instead.
An address may carry a user and a password as well: `nats://user:password@host:4222`.

A `tls://` address or `require_tls(true)` makes the connection use TLS.
`add_root_certificates` adds the certificate authority a private server is verified against, and `add_client_certificate` the certificate and key of mutual TLS.
`tls_client_config` hands the client a whole `rustls` configuration.
The certificate files are read when the broker connects, so a missing one fails [startup](./startup.md).

Logging in and the TLS handshake happen on the server, so a [`TestApp`](../testing/index.md) test starts the same app in process and runs it without them.
The generated [AsyncAPI document](../asyncapi/server.md) reports the host and port of each address and never a credential.

## Example

The service reaches a cluster over TLS and verifies it against its own certificate authority.
The password comes from the environment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/nats_broker_security.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/nats_broker_security.rs#broker"
```

</TabItem>
</Tabs>
