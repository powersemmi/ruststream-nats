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

Medians over eleven interleaved pairs, with the observed spread in parentheses. Higher is better.

| Scenario | Raw client | RustStream | Overhead |
| --- | --- | --- | --- |
| Core NATS, 512 B JSON | 1,635,001 msg/s (1,552,427-1,647,750) | 1,501,334 msg/s (1,490,862-1,529,421) | 8.2% |
| JetStream pull consumer, 512 B JSON, ack each | 234,287 msg/s (232,869-236,999) | 236,224 msg/s (234,639-237,928) | indistinguishable |

The Core NATS row is the framework's own cost and nothing else: a delivery there is a subject match
and a body, and the server settles nothing. A raw delivery costs 612 nanoseconds on this machine
and a delivery through RustStream costs 666, so the subscription stream, the decode and the
dispatch add about 55 nanoseconds to a message.

The JetStream row reports `indistinguishable` because the difference between the two halves is
smaller than the spread between runs of either. Every delivery there carries an acknowledgement
back to the server, and at seven times the cost of a Core delivery it hides a difference this size.
A figure below the run-to-run noise would read as precision that was never measured, so none is
published.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-nats/latest/benchmarks/results.json).

## The machine

| | |
| --- | --- |
| CPU | AMD Ryzen 9 7900X, 12 physical cores, 24 logical |
| Memory | 62.4 GiB |
| OS | Linux 7.2.6 |
| Broker | `nats:2-alpine` in Docker on localhost |
| Rust | 1.98.1, bench profile, no `RUSTFLAGS` |
| Versions | `ruststream-nats` 0.7.0 on `ruststream` 0.7.0-rc.7 |

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one subject, a small body and a server on the loopback. It measures what a
delivery costs in this crate, not what NATS can carry, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last handler returns,
on both halves alike. The framework acknowledges a delivery after the handler is done, which is a
point the handler itself cannot observe, so one acknowledgement out of a million and a half sits
outside the number on both sides.

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
