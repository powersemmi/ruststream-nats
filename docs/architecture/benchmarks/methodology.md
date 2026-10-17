---
title: Methodology
description: "How ruststream-nats measures itself: the three loops, the run on a real server, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-nats` measures itself: the three loops, the run on a real server, the broker-bound
arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on NATS.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `async-nats` driven directly: a Core subscription or a JetStream pull consumer,
  read, decoded and, on JetStream, acknowledged by hand.
- **Broker crate.** This crate and nothing above it: the broker connects, the
  [descriptor](../../components/subscriptions/index.md) opens a subscription, and a loop pulls
  deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the connection options, the subscription, the consumer
configuration, the place of the ack, the decode into the same struct, the payload bytes, the tokio
runtime with four worker threads, and the binary.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh subject per run**, and on JetStream a fresh stream and consumer. A run never sees what
  an earlier one left.
- **The JetStream stream is held in memory, with work-queue retention.** The disk under the server
  stays out of a measurement that is about dispatch.
- **The consumer subscribes first.** A publisher on a second connection then feeds it, and stays at
  most 32768 messages ahead, because Core NATS drops what a slow subscriber cannot take.
- **The window opens at the first delivery and closes when the last one is decoded**, in all three
  loops. Connecting, subscribing and creating the stream are outside it.
- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when the consumer spent most of the run waiting on the server. That is
arithmetic: a probe times 30000 requests the server answers itself (`$JS.API.INFO`) and takes the
median, the transport states how many round trips one delivery costs, and the row is flagged when
their product reaches half the measured time per message. A Core delivery costs no round trip,
because the server pushes it. A JetStream delivery costs one two-hundredth of one, the pull request
for a batch of 200. The round trip is published with the machine, so the arithmetic can be checked.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on
[`NatsBroker`](../../components/broker/index.md) against the same server, built without the
`testing` feature, on a single-threaded runtime. A JetStream pull consumer reads a stream that a
producer on another thread filled before the measured region opens.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

The server is real, so a count moves a little between runs: how the socket hands the client its
bytes depends on timing. Over seven runs the instructions per message moved by less than half a
percent, and the allocations by one block in a run of 2000. A run fails when a scenario allocates
more than the highest total it was seen at plus a tenth of a percent. Against a saved baseline it
also fails on two percent more instructions in a scenario.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both comparison scenarios, stops
the stand and rewrites `docs/benchmarks/results.json`. It takes a few minutes and wants the machine
to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on.
Both recipes build the benchmarks the way a release is built, with fat link-time optimization
across crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes under a minute and needs valgrind. It installs the benchmark runner at the
release `Cargo.lock` pins. `just bench-code --save-baseline=main` on `main` records a baseline,
and `just bench-code --baseline=main` on a branch compares with it. A leading number counts over
another number of deliveries (`just bench-code 5000`); the published chart is counted at 1000.
