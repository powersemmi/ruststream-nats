# Benchmarks

A framework between the NATS client and your handler costs time on every message: the subscription
stream, the decode, the dispatch, the ack. This page says how much, measured against the same work
written by hand on `async-nats`.

Two binaries in one process run the same scenario: one is a RustStream service, the other a loop on
the client. Everything else is held equal - the connection options, the subscription, the consumer
configuration, the position of the ack, the decode into the same type, the payload bytes, the tokio
runtime and the build. The procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

Medians over interleaved pairs, with the observed spread in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "framework": "RustStream", "overhead": "Overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

Core NATS is the framework alone: a delivery there is a subject match and a body, and the server
settles nothing. What RustStream adds to it is the subscription stream, the decode and the dispatch.

A row reported as `indistinguishable` is one whose two halves differ by less than the spread between
runs of either. That is the honest outcome wherever the transport costs far more than the dispatch:
every JetStream delivery carries an acknowledgement back to the server, and a difference this small
disappears inside one. A figure below the run-to-run noise would read as precision that was never
measured, so none is published.

A row marked `broker-bound` is one where the transport makes the consumer wait for the server
often enough to account for half of what a message costs. There the number says more about the
server and the loopback than about this crate. Core NATS charges the consumer no round trip per
delivery: the server pushes a match down the subscription. A JetStream pull consumer charges one
pull request per batch, and an acknowledgement it sends without waiting for an answer. The round
trip those are counted against is the probe below the machine, so the sum can be redone.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-nats/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one subject, a small body and a server on the loopback. It measures what a
delivery costs in this crate, not what NATS can carry, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last handler returns,
on both halves alike. The framework acknowledges a delivery after the handler is done, which is a
point the handler itself cannot observe, so one acknowledgement out of the millions a run carries
sits outside the number on both sides.

The JetStream figure is taken on a memory-backed stream with work-queue retention. That keeps the
disk under the server out of a measurement that is about dispatch; a stream on a file store answers
a different question, and answers it about the server rather than about this crate.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about ten minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.
