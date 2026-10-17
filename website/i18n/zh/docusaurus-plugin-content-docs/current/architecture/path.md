---
title: 路径
sidebar_position: 4
description: "一条 NATS 消息在 RustStream 服务中的路径：订阅、投递、处理器、应答和结算，分别在 Core 和 JetStream 上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条得到应答的消息的路径，左边是 Core NATS，右边是 JetStream，中间经过同一条运行时路径",
  "text": "以文字阅读本图",
  "coreSource": ["Core 订阅", "由服务器推送"],
  "jetStreamSource": ["拉取消费者", "每批 200 条"],
  "coreMessage": ["NatsMessage::Core", "消息头只转换一次"],
  "jetStreamMessage": ["NatsMessage::JetStream", "元数据按需读取"],
  "dispatch": ["分发", "一个循环，或每个键一个工作分区"],
  "decode": ["解码", "直接从载荷读取"],
  "body": ["订阅者处理器", "#[subscriber] 下的 async fn"],
  "encode": ["应答", "编码进一个缓冲区"],
  "publisher": ["NatsPublisher", "把缓冲区交给客户端"],
  "coreSettle": ["无需结算", "AckError::Unsupported"],
  "jetStreamSettle": ["+ACK", "发送后不等待"]
};

# 路径

一条 NATS 消息在 RustStream 服务中的路径：订阅、投递、处理器、应答和结算，分别在 Core 和 JetStream 上。

<Diagram name="path" words={words} />

Core 消息由服务器推送到订阅中。JetStream 消息作为拉取请求的应答到达，`async-nats` 每次请求 200 条消息。无论哪种方式，[`NatsMessage`](api:ruststream_nats::NatsMessage) 都把客户端的消息原样保留，载荷也一样。

消息头在生成投递时转换成框架的映射，只转换一次。没有消息头的消息完全不构建映射。在 Core 上，请求的应答 subject 作为 `reply-to` 消息头加入。在 JetStream 上，[元数据](../components/context/index.md)在处理器请求时才从确认 subject 中读取，此前不读。

处理器的应答编码进一个缓冲区，[发布者](../components/publishing/index.md)把这个缓冲区原样交给 `async-nats`，不做复制。Core 发布在客户端接收消息后返回。[JetStream 发布](../components/publishing/jetstream-publish/index.md)在流确认消息后返回。

投递在应答之后结算。JetStream 确认是发往确认 subject 的一条消息，发送后不等待应答。Core 投递无需结算：调用报告 `AckError::Unsupported`，运行时记录日志。每种结果做什么，见[结算](settlement.md)。

[基准测试](benchmarks/index.md#cost-of-the-code)统计的正是这条路径：一次 JetStream 投递的指令数和内存分配次数，包括客户端在服务线程上所做的工作。
