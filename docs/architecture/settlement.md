---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on NATS: an acknowledgement the JetStream server acts on, and copies the runtime publishes on Core."
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "Each outcome of a handler on a JetStream delivery: what the delivery sends and what the server does",
  "text": "The diagram as text",
  "outcomeLane": "Handler",
  "wireLane": "Sent",
  "serverLane": "Server",
  "ack": ["ack()"],
  "retry": ["retry()"],
  "retryAfter": ["retry_after(d)"],
  "drop": ["drop()"],
  "unsettled": ["no outcome", "before ack_wait"],
  "nothing": "nothing",
  "ackEffect": ["finished,", "never sent", "again"],
  "retryEffect": ["sent again", "now, delivered", "+ 1"],
  "retryAfterEffect": ["held for d,", "then sent again,", "delivered + 1"],
  "dropEffect": ["ended, never", "sent again"],
  "unsettledEffect": ["sent again after", "ack_wait,", "delivered + 1"],
  "core": ["On Core NATS every outcome reports AckError::Unsupported:", "the server settles nothing"]
};

export const retries = {
  "title": "A retry on NATS: the attempt cap decides between bringing the message back and the dead-letter subject",
  "text": "The diagram as text",
  "delivery": ["A delivery", "JetStream counts its deliveries; Core carries a header"],
  "outcome": ["The handler returns", "retry_after(delay), or retry() under a declaration"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "below": "Below the cap",
  "nak": ["JetStream:", "-NAK with the delay,", "the server holds it"],
  "copy": ["Core subject:", "a copy published", "to the subject"],
  "named": ["Core pattern:", "a copy to the subject", "the mount site names"],
  "atCap": "At the cap",
  "deadLetter": ["Dead-letter subject:", "the delivery", "published as it", "arrived"],
  "terminated": ["No dead letter:", "+TERM on JetStream,", "nothing more", "on Core"],
  "lost": ["A Core copy waits for its delay inside the process:", "a stop before the timer fires loses it"],
  "handled": "handled",
  "retries": "a retry",
  "belowCap": "below",
  "reached": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and [`retry_after`](../components/settlement/retry-after.md) become on
NATS: an acknowledgement the JetStream server acts on, and copies the runtime publishes on Core.

<Diagram name="settlement" words={table} />

A JetStream delivery settles on the server. The answer goes to the delivery's acknowledgement
subject, and the consumer acts on it as [Consumers](nats/consumers.md) draws. A `retry_after` puts
the delay into the negative acknowledgement itself. The server holds the message and delivers it
again on the same consumer, with the same stream sequence and a `delivered` count one higher.
Nothing is published, and no copy is made.

A Core delivery has nothing to settle. `ack`, `retry` and `drop` report `AckError::Unsupported`,
which the runtime logs. The server keeps nothing to deliver again, and the runtime brings a message
back only as a copy it publishes. That is
[at-most-once](../components/settlement/at-most-once.md) delivery.

| Outcome | Core NATS |
|---|---|
| `ack`, `drop` | `AckError::Unsupported` |
| `retry()` | `AckError::Unsupported`; under a declared cap, a copy published at once |
| `retry_after(d)` | a copy published after `d` |
| no outcome | nothing |

## Retries

<Diagram name="retries" words={retries} />

The cap is declared at the mount site with
[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts), and the first delivery counts
as one. On JetStream the count is the server's own `delivered`, so a delivery that ran past
`ack_wait` counts too. On Core the runtime counts the copies it published, in the copy's
`x-ruststream-retry-count` header.

Below the cap, each model brings the message back its own way. JetStream redelivers it from the
stream. On Core the runtime publishes a copy to the address the descriptor reports. A
[`CoreSubject`](../components/subscriptions/core-subject.md) reports its own subject. A
[`CoreWildcard`](../components/subscriptions/core-wildcard.md) matches many subjects and
publishes to none, so its registration [names the subject](../components/settlement/out-retry.md)
a copy goes to, or the app refuses to start.

At the cap, the delivery goes to the [dead-letter subject](../components/settlement/dead-letters.md)
as it arrived, payload and headers unchanged. With no dead-letter subject declared, a JetStream
delivery is terminated with `+TERM`, and a Core one is let go.

The crate declares nothing to the server: `max_deliver` stays unset, because JetStream has no
place to send a message that ran out of deliveries. The runtime's path covers both models.
