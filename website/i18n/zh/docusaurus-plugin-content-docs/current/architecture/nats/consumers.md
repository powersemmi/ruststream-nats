---
title: 消费者
sidebar_position: 2
description: "JetStream 拉取消费者按客户端的请求成批交出消息，并等待对每条消息的应答：确认、否定确认或终止。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "流 ORDERS 的一个拉取消费者：客户端请求一批消息，收到消息后用 +ACK、-NAK 或 +TERM 应答每一条",
  "text": "以文字阅读本图",
  "stream": "流 ORDERS",
  "consumer": ["拉取消费者 billing", "流中的一个位置、一个过滤 subject、", "投递策略、ack_wait、max_ack_pending"],
  "client": ["订阅", "一次取一条消息"],
  "ack": ["+ACK", "完成：不会", "再次投递"],
  "nak": ["-NAK", "立即或在", "一段延迟后", "再次投递"],
  "term": ["+TERM", "结束：不会", "再次投递"],
  "redelivery": ["超过 ack_wait 仍未应答的消息会重新投递，", "它的投递次数加一"],
  "messages": "消息",
  "pull": "拉取：批大小、期限"
};

# 消费者

JetStream 拉取消费者按客户端的请求成批交出消息，并等待对每条消息的应答：确认、否定确认或终止。

<Diagram name="consumers" words={words} />

消费者是某一个流的视图，有它自己的位置。它的过滤 subject 限定它读取流中的哪些消息。它的投递策略决定新消费者从哪里开始：第一条消息、最后一条、只读新消息、某个序号或某个时间。持久化消费者有名字，重启后依然存在，绑定到这个名字的每个客户端共用它。临时消费者没有名字，一旦没有人读取，服务器就删除它。

客户端不请求，拉取消费者就什么也不发送。拉取请求给出批大小和期限，服务器在期限之前用不超过这个数量的消息应答。`async-nats` 让请求持续不断：它每次请求 200 条消息，取走一半时再次请求，等待期间每 15 秒期望收到一次心跳。

每条消息都等待一个应答。`+ACK` 完成它。`-NAK` 让它立即再次投递，或在它指定的延迟之后再次投递。`+TERM` 结束它，不再投递。在 `ack_wait` 内没有应答的消息会重新投递，每次投递都让它的 `delivered` 计数加一。`max_ack_pending` 限制同时等待应答的消息数量：到达上限时，消费者不再交出消息。

每次投递的确认 subject 携带它的元数据：流和消费者的名字、流序号和消费者序号、`delivered` 计数，以及仍在等待的消息数。

`ruststream-nats` 在服务启动时创建消费者，并显式发送 `ack_wait`（30 秒）、`max_ack_pending`（1024）和投递策略（全部消息）。其余配置保持服务器的默认值。

这一模型就是 NATS 服务器自己的模型（[消费者](https://docs.nats.io/nats-concepts/jetstream/consumers)和[确认模型](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#acknowledgement-models)）。
