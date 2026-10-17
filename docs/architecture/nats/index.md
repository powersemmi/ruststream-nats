---
title: Subjects
sidebar_position: 0
description: "Core NATS delivers a message to every subscription whose subject matches at the moment it arrives, to one member of a queue group, and stores nothing."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One publish to orders.eu.created: the server copies it to each matching subscription and to one member of a queue group",
  "text": "The diagram as text",
  "publisher": "Publisher",
  "server": ["NATS server", "matches the", "subject against", "every", "subscription", "it holds now"],
  "exact": "the same subject",
  "one": "* is one token",
  "rest": "> is the rest",
  "other": "no match",
  "queue": "Queue group billing",
  "member1": "member 1 gets it",
  "member2": "member 2 does not",
  "dropped": ["A message no subscription matches is dropped:", "Core NATS stores nothing"],
  "publishes": "publish"
};

# Subjects

Core NATS delivers a message to every subscription whose subject matches at the moment it arrives,
to one member of a queue group, and stores nothing.

<Diagram name="subjects" words={words} />

A subject is a string of tokens separated by dots. A subscription subject may hold wildcards. `*`
stands for exactly one token. `>` stands for one or more tokens and comes last. A publish always
names a full subject, without wildcards.

The server copies a message to each subscription that matches it. A subscription that joins later
never sees it, and a message nothing matches is gone. That is at-most-once delivery: the server
keeps nothing and settles nothing.

A queue group shares the work. The server hands each message to one member of the group, and a
subscription outside the group still gets its own copy. The group is identified by its name alone,
so members subscribed to different subjects under one name form one group.

A request is a publish that names a reply subject, an inbox the requester subscribed to first. The
responder publishes the answer there. A request that no subscription matches gets an immediate "no
responders" answer, so the requester does not wait for its timeout.

A message carries headers since NATS 2.2. A header name is printable ASCII without a colon, and a
value is a single line. The server refuses a message over its payload limit, 1 MiB by default.

The model is the NATS server's own
([subjects](https://docs.nats.io/nats-concepts/subjects),
[publish-subscribe](https://docs.nats.io/nats-concepts/core-nats/pubsub),
[queue groups](https://docs.nats.io/nats-concepts/core-nats/queue),
[request-reply](https://docs.nats.io/nats-concepts/core-nats/reqreply)). How a stream keeps
messages is on [Streams](streams.md), and how a consumer reads them on
[Consumers](consumers.md).
