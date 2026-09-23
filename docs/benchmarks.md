# Benchmarks

A framework between the NATS client and your handler costs time on every message: the subscription
stream, the decode, the dispatch, the ack. This page says how much, measured against the same work
written by hand on `async-nats`.

One process runs the same scenario three ways. **Raw** drives `async-nats` directly. **Adapter**
drives this crate's own types by hand, with no handler and no runtime above them. **Service** is the
application a user writes. Everything else is held equal - the connection options, the subscription,
the consumer configuration, the position of the ack, the decode into the same type, the payload
bytes, the tokio runtime and the build. The procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

Two differences come out of that. Adapter against raw is what this crate's consumer and publisher
cost over the client they wrap: the number this repository answers for. Service against raw is what
a whole service costs, adapter and runtime together. What the runtime costs on its own is the
distance between the two columns, and it is published here per broker because a runtime share that
differs between brokers is a fact about how the transport and the runtime meet.

## The numbers

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "ruststream-nats", "framework": "RustStream service", "adapterOverhead": "Adapter over raw", "overhead": "Service over raw", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "versions": "Versions", "measured": "Measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

Core NATS is the thinnest thing the numbers can sit on: a delivery there is a subject match and a
body, and the server settles nothing, so what each column adds to the one before it is visible
without a transport cost around it.

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

## The crate's own code

<div id="benchmark-code"></div>

The second table is counted rather than timed: instructions under callgrind and allocations under
DHAT. Each scenario is the service a user writes, started on `NatsTestBroker`, the crate's
in-process transport, so no socket and no server are in the number. The transport resolves the
crate's subscription descriptors and pairs its publish policies, the ones a service ships with.
Its message, its subscriber, its subject matching and its publisher are its own, and its router
keeps a log of every publish for the tests to read back. The conversions between `async-nats` and
the framework are measured in the table above, together with the client.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what starting the service and taking the first
delivery cost once. The numbers are absolute, the framework's own cost included; the core publishes
that cost alone on its [benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
The reply row counts the in-process publisher, which checks the subject, matches it against every
subscription and copies the message into its log.

A count repeats within a tenth of a percent between runs of one binary, so a change to this path
shows in it however small. `just bench-code` fails on an allocation above the floor a scenario
declares, and with `--baseline=main` on more than two percent more instructions, and a pull
request that changes the cost cites its numbers. The in-process transport is compiled with the
`testing` feature, which brings the framework's test hooks with it. Outside a test they stay
empty, and a single delivery allocates nothing for them. A batch copies each payload twice for the
harness record whether a test runs or not, and those copies are two of the batch row's allocations
per message.

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

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about ten minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.

```bash
just bench-code
```

The recipe counts the code table under valgrind and rewrites the `code` section of the same
document. It takes seconds and needs no stand, only valgrind and the benchmark runner:
`cargo install --locked gungraun-runner --version =0.19.4`.
