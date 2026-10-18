---
title: 生命周期
sidebar_position: 3
description: "NatsBroker::new 只记录配置，connect 拨号，启动时为每个 subject 订阅或为每个消费者创建，关闭时排空。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的三个状态作为类型，以及每次转换在 NATS 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "nats": "在 NATS 上",
  "unconnected": ["NatsBroker::new", "(addresses)"],
  "connected": ["ConnectedNatsBroker"],
  "closed": ["ClosedNatsBroker", "排空后的计数器"],
  "config": ["地址和选项，", "无 I/O"],
  "dial": ["用连接选项", "拨号"],
  "core": ["每个 Core 订阅：", "SUB，然后 flush"],
  "jetstream": ["每个 JetStream 订阅：", "查找流，", "创建消费者"],
  "drain": ["标为已关闭，", "然后排空"],
  "afterShutdown": ["比连接活得更久的发布者", "返回 NatsError::Closed，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`NatsBroker::new`](../components/broker/index.md) 只记录配置，connect 拨号，启动时为每个 subject 订阅或为每个消费者创建，关闭时排空。

<Diagram name="lifecycle" words={words} />

`new` 不做任何 I/O，所以服务的构建器保持同步。它接受一个地址或以逗号分隔的地址列表，[`with_options`](../components/broker/connect-options.md) 附加 `async-nats` 的连接选项。运行时在应用启动时连接 Broker。connect 用这些选项拨号。服务器不可达时，启动以 `NatsError::Connect` 失败。

每个订阅的[描述符](../components/subscriptions/index.md)针对已连接的 Broker 解析一次。Core 订阅发送 `SUB`，然后等待一次 flush 往返。到那时服务器已经登记了这个订阅，所以另一个连接上的生产者不会把消息发布到空当里。JetStream 订阅按名字查找它的流，并创建它的拉取消费者，这两个请求都由服务器应答。流不存在时，启动以 `NatsError::JetStream` 失败。

[关闭](../components/broker/shutdown.md)先把连接标为已关闭，这样共用这个连接的发布者就无法再塞进一条消息。然后它排空客户端：订阅停止，已经发布的内容被刷出，连接关闭。[`ClosedNatsBroker`](api:ruststream_nats::ClosedNatsBroker) 携带客户端的计数器：发送的消息数、接收的消息数和连接次数。

关闭之前配对的发布者是关闭收不回的句柄。它返回 `NatsError::Closed`，并附上它被要求发布到的 subject。

进程内传输遵循同样的阶梯。它像 connect 那样解析地址，并从连接选项中读取 `no_echo` 和 inbox 前缀。
