---
title: 流
sidebar_position: 1
description: "JetStream 流以序号存储发布到它的 subject 的消息，确认每一次发布，并由各个消费者从各自的位置读取。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一次发布存入流 ORDERS，序号为 6，并得到确认；读取它的是一个有两个成员的持久化消费者和一个临时消费者",
  "text": "以文字阅读本图",
  "publisher": ["发布者", "orders.created,", "Nats-Msg-Id"],
  "ack": ["PubAck", "流 ORDERS，", "序号 6，不是重复消息"],
  "stream": "流 ORDERS",
  "dedup": ["最近两分钟内出现过的 Nats-Msg-Id", "会再次得到确认，但只存储一次"],
  "durable": "持久化消费者 billing",
  "replica1": "副本 1",
  "replica2": "副本 2",
  "ephemeral": ["临时", "消费者", "不活动时", "被删除"],
  "positions": ["每个消费者在流中保有自己的位置；", "持久化消费者的各个成员共用一个位置"],
  "publish": "发布",
  "acknowledged": "PubAck",
  "reads": "读取"
};

# 流

JetStream 流以序号存储发布到它的 subject 的消息，确认每一次发布，并由各个消费者从各自的位置读取。

<Diagram name="streams" words={words} />

流捕获发布到它所列出的 subject 的消息，这里是 `orders.*`。它按顺序给消息编号，并一直保存，直到保留策略删除它们。删除的依据可以是条数、大小和存在时间的上限，可以是不再有消费者对它们感兴趣，也可以是工作队列的消费者确认了它们。

发布者为每条消息得到一个应答：流的名字、序号，以及这条消息是否重复。没有流捕获的发布会得到一个错误，而不是 Core NATS 那样的沉默。

消息头声明服务器在存储消息之前要检查什么。`Nats-Msg-Id` 给消息命名。在流的去重窗口内（默认两分钟）重复出现的消息会作为重复消息得到确认，并且只存储一次。`Nats-Expected-Stream`、`Nats-Expected-Last-Sequence`、`Nats-Expected-Last-Subject-Sequence` 和 `Nats-Expected-Last-Msg-Id` 让服务器在流的状态不符合发布者预期时拒绝这次发布。

`ruststream-nats` 不创建流。流由 `nats` 命令行工具或 JetStream API 创建和配置，服务启动时订阅按名字找到它。

这一模型就是 NATS 服务器自己的模型（[流](https://docs.nats.io/nats-concepts/jetstream/streams)、[消息头](https://docs.nats.io/nats-concepts/jetstream/headers)和[去重](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive#message-deduplication)）。消费者如何读取流，见[消费者](consumers.md)。
