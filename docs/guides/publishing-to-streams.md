---
sidebar_position: 5
description: "Publish replies and messages into JetStream streams, store a repeated message once with a message id, and refuse a write made from a stale version."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publishing to streams

This guide publishes replies and messages into JetStream streams, stores a repeated message once, and refuses a write made from a stale version.

A plain publish goes out over Core NATS and nobody confirms it.
[`JetStreamPublish`](../components/publishing/jetstream-publish/index.md) waits for the stream to acknowledge each message.
So a message the stream refuses is an error the handler sees, not a silent loss.

The example is an orders service.
It confirms each order, archives it, and keeps the history of each customer account.
The streams it writes to exist on the server:

```bash
nats stream add ORDERS --subjects 'orders.placed' --defaults
nats stream add CONFIRMATIONS --subjects 'confirmations.orders' --defaults
nats stream add ARCHIVE --subjects 'archive.orders' --defaults
nats stream add ACCOUNTS --subjects 'accounts.*' --defaults
```

## A reply into a stream

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#confirm"
```

</TabItem>
</Tabs>

`confirm` answers each order with a `Confirmation` on `confirmations.orders`.
The app below sends that reply through `JetStreamPublish::default().expect_stream("CONFIRMATIONS")`.
[`expect_stream`](../components/publishing/jetstream-publish/expect-stream.md) names the stream that must store the subject, and a publish that another stream would take is refused.
The order is acknowledged only after the stream has stored its confirmation.
A refused confirmation fails the delivery, and the order comes back.

The reply's subject is outside the `ORDERS` subjects.
A reply stored in the stream a handler reads would reach that handler again.

## Storing a message once

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#archive"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#archive"
```

</TabItem>
</Tabs>

An order can be delivered twice, and then `archive` publishes its copy twice.
[`message_id`](../components/publishing/jetstream-publish/message-id.md) gives every copy of one order the same id, which travels as the `Nats-Msg-Id` header.
The stream stores the first copy and acknowledges a repeat without storing it, so `index` reads each order once.
The stream remembers ids for its duplicate window, two minutes unless the stream sets another.

`message_id` describes one message, so it is a step of the publish, not a setting of the mount site.
The steps come from [`JetStreamPublishSteps`](api:ruststream_nats::JetStreamPublishSteps) in the crate's prelude.
A body that calls one bounds its slot `Out<impl Publisher<Options = JetStreamOptions>, _>`.
That bound ties the handler to JetStream, which is the one place a handler names the broker.

## Refusing a stale write

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#accounts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#accounts"
```

</TabItem>
</Tabs>

The history of account 42 is the stream's subject `accounts.42`.
A client changes the account's e-mail from the version it last read: the stream sequence of the account's last event.
[`expect_last_subject_sequence`](../components/publishing/jetstream-publish/expectations.md) makes the stream store the event only if the subject's last message is still at that sequence.
Two clients that read the same version cannot both write: the second write is refused, and `change_email` drops it.
`expect_last_sequence` checks the whole stream instead of one subject, and `expect_last_message_id` checks the stream's last message id.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#app"
```

</TabItem>
</Tabs>

Each JetStream publish waits for the stream's acknowledgement.
Outside a delivery, a startup hook takes the policy and [`publish_ack`](../components/publishing/jetstream-publish/publish-ack.md) returns that acknowledgement: the stream, the sequence, and whether the duplicate window had seen the message.
A Core NATS publish is the right choice where losing a message is acceptable, such as the dashboard of [Acknowledgement and delivery guarantees](acknowledgement.md).

The tests in the example file run the app in process: the in-process stream checks message ids and expectations as a server does.
