---
title: Path
sidebar_position: 4
description: "The way of one NATS message through a RustStream service: the subscription, the delivery, the handler, the reply and the settlement, on Core and on JetStream."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one message that is answered, on Core NATS on the left and on JetStream on the right, through one shared runtime path",
  "text": "The diagram as text",
  "coreSource": ["Core subscription", "the server pushes"],
  "jetStreamSource": ["Pull consumer", "batches of 200"],
  "coreMessage": ["NatsMessage::Core", "headers converted once"],
  "jetStreamMessage": ["NatsMessage::JetStream", "metadata read on demand"],
  "dispatch": ["Dispatch", "one loop, or a lane per key"],
  "decode": ["Decode", "read in place from the payload"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into a buffer"],
  "publisher": ["NatsPublisher", "hands the buffer to the client"],
  "coreSettle": ["Nothing to settle", "AckError::Unsupported"],
  "jetStreamSettle": ["+ACK", "sent without waiting"]
};

# Path

The way of one NATS message through a RustStream service: the subscription, the delivery, the
handler, the reply and the settlement, on Core and on JetStream.

<Diagram name="path" words={words} />

A Core message is pushed by the server into the subscription. A JetStream message arrives in
answer to a pull request, which `async-nats` sends for 200 messages at a time. Either way the
[`NatsMessage`](api:ruststream_nats::NatsMessage) keeps the client's message as it came, payload
included.

The headers are converted to the framework's map once, when the delivery is made. A message
without headers builds no map at all. On Core, a request's reply subject is added as the `reply-to`
header. On JetStream, the [metadata](../components/context/index.md) is read from the
acknowledgement subject when a handler asks for it, and not before.

The handler's reply is encoded into a buffer, and the
[publisher](../components/publishing/index.md) hands that buffer over to `async-nats` as it is,
without a copy. A Core publish returns once the client has taken the message. A
[JetStream publish](../components/publishing/jetstream-publish/index.md) returns once the stream has
acknowledged it.

The delivery settles after the reply. A JetStream acknowledgement is one message to the
acknowledgement subject, sent without waiting for an answer. A Core delivery has nothing to settle:
the call reports `AckError::Unsupported` and the runtime logs it. What each outcome does is on
[Settlement](settlement.md).

The [benchmarks](benchmarks/index.md#cost-of-the-code) count this path: the instructions and
allocations of one JetStream delivery, the client's work on the service's thread included.
