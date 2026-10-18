---
title: 设计
sidebar_position: 2
description: "ruststream-nats 的位置：在 RustStream 运行时与 async-nats 客户端之间，所有订阅和发布者都在同一个连接上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、async-nats 客户端和 NATS 服务器",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算、重试",
  "crate": "ruststream-nats",
  "subscriber": ["NatsSubscriber", "产出 NatsMessage"],
  "publishers": ["NatsPublisher,", "JetStreamPublisher"],
  "context": "上下文键",
  "inProcess": "进程内传输",
  "broker": ["NatsBroker", "一个连接"],
  "descriptors": ["CoreSubject,", "CoreWildcard,", "JetStreamSubject"],
  "client": "async-nats",
  "jetstream": "客户端上的 jetstream::Context",
  "connection": "Client：一个连接",
  "server": "NATS 服务器",
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-nats` 所处的位置：在 RustStream 运行时与 `async-nats` 客户端之间，所有订阅和发布者都在同一个连接上。

<Diagram name="design" words={words} />

本 crate 在 [`async-nats`](https://docs.rs/async-nats/0.50.0/async_nats/) 之上实现核心的 Broker trait，不给协议增加任何东西。它上面的运行时对每个 Broker 都相同。NATS 特有的一切都在本 crate 里：[描述符](../components/subscriptions/index.md)、[ack 的含义](../components/settlement/index.md)、[重试副本发往何处](../components/settlement/retries.md)，以及[投递的元数据](../components/context/index.md)。

已连接的 Broker 持有一个客户端，由它配对出的每个发布者都共用这个客户端。一个 Core 订阅就是连接上的一次订阅。一个 JetStream 订阅创建一个拉取消费者，并通过同一个连接读取它的消息。因此，服务按处理器和按发布选择 Core 还是 JetStream，而不是按进程选择。

JetStream 上下文建立在 Broker 连接时所在的运行时上。客户端为它启动的任务，即消费者的拉取循环和确认任务，无论哪个线程发出请求，都在那里运行。

本 crate 没有为之提供类型的设置，就是一个 `async-nats` 连接选项，由 [`with_options`](../components/broker/connect-options.md) 在任何拨号之前附加。[`client`](api:ruststream_nats::ConnectedNatsBroker::client) 和 [`jetstream`](api:ruststream_nats::ConnectedNatsBroker::jetstream) 交出客户端和一个 JetStream 上下文，用于本 crate 没有封装的操作，比如创建流。[`from_client`](api:ruststream_nats::ConnectedNatsBroker::from_client) 接管一个在别处构建的客户端。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。

## 进程内传输 {/* #the-in-process-transport */}

启用 `testing` 特性后，[`TestApp::start`](../components/testing/index.md) 把同一个 Broker 连接到进程内传输，而不是服务器。描述符、策略和投递都是生产环境中的那些。它用 `*` 和 `>` 匹配 subject，让队列组分担消息，用 "no responders" 应答请求，并保存流及其序号、去重窗口和发布所声明的预期。它的消费者从各自的投递策略开始投递，报告元数据，并像服务器那样结算、延迟，以及在 `ack_wait` 之后重新投递。

属于服务器本身的东西留给真实服务器上的运行：流的 subject 列表和上限、`max_ack_pending`、订阅的等待上限、认证和 TLS。在进程内，订阅能找到它指定的每个流；对着服务器时，不存在的流会让启动失败。

不启用该特性时，这个传输不参与编译。生产构建中已连接的 Broker 就是一个指向连接的共享指针，编译器会检查这个大小。
