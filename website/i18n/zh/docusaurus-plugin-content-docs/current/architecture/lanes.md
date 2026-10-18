---
title: 工作分区
sidebar_position: 5
description: "一个订阅一次交出一条消息；workers(n, by_key) 按 nats-partition-key 消息头把消息分到各个工作分区上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：一条消息流，以及按分区键消息头划分的两个工作分区",
  "text": "以文字阅读本图",
  "subscription": ["订阅", "一个 Core subject 或一个拉取消费者"],
  "stream": ["NatsSubscriber 流", "一次一条消息"],
  "keyed": ["workers(2, by_key)", "工作分区 = nats-partition-key 的哈希"],
  "lane0": ["工作分区 1", "键 a，按顺序"],
  "lane1": ["工作分区 2", "键 b，按顺序"],
  "noKey": ["没有该消息头的消息就没有键：", "这样的消息轮流分配到各个工作分区"],
  "scale": ["跨进程：队列组或共用的持久化消费者", "把每条消息交给一个成员"],
  "delivered": "投递",
  "oneAtATime": "一次一条"
};

# 工作分区

一个订阅一次交出一条消息；`workers(n, by_key)` 按 `nats-partition-key` 消息头把消息分到各个工作分区上。

<Diagram name="lanes" words={words} />

不用 `workers` 时，一个循环依次为每条消息运行处理器。订阅的顺序得以保持，也没有任何东西并行运行。

`workers(n, by_key)` 运行 `n` 个工作分区。消息进入其键的哈希所对应的工作分区，每个工作分区按顺序运行自己的消息。NATS 没有自己的分区，所以键就是 [`nats-partition-key`](../components/subscriptions/keys.md) 消息头，由发送方设置。同一个键的消息保持顺序，不同的键并行运行。

跨进程时，由服务器分担工作。[队列组](../components/subscriptions/queue-group.md)把每条 Core 消息交给一个成员，顺序由服务器决定。绑定同一个[持久化](../components/subscriptions/durable.md)消费者的订阅轮流取得每条 JetStream 消息。[`max_ack_pending`](../components/subscriptions/max-ack-pending.md) 限制一个消费者在它的所有成员中同时等待应答的消息数。

[批处理器](../components/subscriptions/batches.md)最多接收挂载点指定的大小。在 JetStream 上，一个批就是一次拉取，`pull_expires` 到期时提前结束。在 Core 上，批在客户端组装，达到该大小或第一条消息之后 10 毫秒时结束。
