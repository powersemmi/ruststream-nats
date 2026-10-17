---
title: Numbers
description: "What the benchmark numbers of ruststream-nats measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-nats` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one server. A row is not comparable with a row another broker crate publishes: the
transports do different work per message.

The server runs on the same machine, on the loopback. With no network latency, the same cost per
message is a larger share of the time than it would be against a server across a network. Read the
percentage as an upper bound on what a deployed service pays.

A row reported as `indistinguishable` is one whose loops differ by less than the spread between
their rounds. A figure below that noise would read as precision that was never measured, so none is
published. On JetStream every delivery sends an acknowledgement back to the server, and a
difference this small disappears inside it.

A `broker-bound` row is one where the consumer waited on the server for most of the run. The work
of the crate and the runtime then happens inside that wait, and the difference comes out near zero.
That is a lower bound on the cost, not a measurement of it.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the `async-nats` work on
the service's thread, its connection task included. It moves a little between runs, because the
server is real. The same count costs more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Connecting, finding the stream, creating the consumer and the first allocations behind them are the
cold start, paid once.

The allocations of a JetStream delivery are mostly the client's: a pull request for every hundred
messages it hands out, and the blocks of the channel between its connection task and the
subscription.

## What they do not mean

A row is one consumer reading one subject, a small body and a single server. It measures what a
delivery costs inside this crate, not what NATS can carry.

The JetStream stream is held in memory. A stream on a file store answers a different question,
about the server rather than about this crate. A queue group, a pool of worker lanes and several
consumers on one stream each answer a different question too, and the comparison measures none of
them. The code chart counts a [batch handler](../../components/subscriptions/batches.md) and a
reply on the same stand.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
