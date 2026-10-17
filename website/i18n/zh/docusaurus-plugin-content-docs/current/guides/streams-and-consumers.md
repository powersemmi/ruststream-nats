---
title: 流与消费者
sidebar_position: 3
description: "通过持久化和临时拉取消费者读取 JetStream 流，选择消费者的起点，读取投递的元数据，并成批接收消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 流与消费者

本指南通过持久化消费者和临时消费者读取 JetStream 流，选择新消费者从哪里开始，读取投递的元数据，并成批接收消息。

流保存发布到它的 subject 上的消息，每条消息都有一个序号。
消费者按自己的节奏读取流，并逐条确认消息。

示例是一个网店。
它履行新订单，在看板上显示每个订单事件，并与银行核对已付款的订单。

## 流 {/* #the-stream */}

流是服务器的状态。
服务在启动时查找它，从不创建它，所以要用 NATS 命令行工具创建一次：

```bash
nats stream add ORDERS --subjects 'orders.*' --defaults
```

`ORDERS` 保存发布到 `orders.created`、`orders.paid` 以及 `orders.*` 匹配的其他任何 subject 上的每条消息。
向这样的 subject 做一次普通发布，消息就会被保存，不论发布者是谁。
订阅服务器上不存在的流，会让服务启动失败。

## 持久化消费者 {/* #a-durable-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#fulfil"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#fulfil"
```

</TabItem>
</Tabs>

[`JetStreamSubject`](../components/subscriptions/jetstream-subject.md) 接收消费者读取的 subject 和保存它的流。
subject 就是消费者的过滤条件，所以 `fulfil` 只从 `ORDERS` 中读取 `orders.created`。
两者需要不同时，[`filter_subject`](../components/subscriptions/filter-subject.md) 设置另一个过滤条件。
服务在启动时创建消费者，带上描述符的每一项设置。

[`durable`](../components/subscriptions/durable.md) 把消费者命名为 `fulfilment`。
服务器保存它的位置，所以重启后的服务从停下的地方继续。
服务的各个副本共用这一个持久化消费者，每条消息交给其中一个副本。
这就是 JetStream 中与[队列组](subjects-and-queue-groups.md)相对应的机制。

处理器用 [`Ctx<StreamSequence>`](../components/context/index.md) 读取投递在流中的位置。
[JetStream 上下文](../components/context/index.md)还带有流和消费者的名字、消费者序号、投递次数，以及仍在等待的消息数。
在手写形式中，处理器接收 `Context<'_, JetStreamContext>`，用 `ctx.context(..)` 读取同样的键。
在 Core NATS 投递上，每个键读出的都是 `None`。

## 临时消费者 {/* #an-ephemeral-consumer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#dashboard"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#dashboard"
```

</TabItem>
</Tabs>

没有 `durable` 的描述符创建一个临时消费者。
每个进程得到自己的消费者，订阅关闭后服务器删除它。
所以服务的每个副本都显示每个订单事件。

[`DeliverPolicy`](../components/subscriptions/deliver-policy.md) 决定新消费者从哪里开始。
默认的 `All` 从流保存的第一条消息开始，`New` 从下一条发布的消息开始。
`Last`、`LastPerSubject`、`ByStartSequence` 和 `ByStartTime` 分别从最后一条消息、每个 subject 的最后一条消息、某个序号或某个时间点开始。
该策略只在创建消费者时生效一次；之后，持久化消费者从自己的位置继续。

## 批 {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#reconcile"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#reconcile"
```

</TabItem>
</Tabs>

接收 `&[Order]` 的处理器得到一[批](../components/subscriptions/batches.md)消息，并为整批只运行一次。
它返回的结果结算这批中的每条消息。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#app"
```

</TabItem>
</Tabs>

挂载点指定批的大小：`batch(100)` 在一次拉取中向服务器请求最多一百条消息。
描述符上的 [`pull_expires`](api:ruststream_nats::JetStreamSubject::pull_expires) 决定一次拉取等待它们多久，这里是一秒，之后交出一个较小的批。
默认值是五秒。

`fulfil` 的回复通过 Core NATS 发布，这是 Broker 的默认方式。
[向流发布](publishing-to-streams.md)则把回复保存到流中。
Core 订阅和 JetStream 订阅共用一个连接，所以一个服务可以自由混用两种模型。

[`max_ack_pending`](../components/subscriptions/max-ack-pending.md) 限制一个消费者同时持有的未确认投递数；默认值是 1024。
[确认与投递保证](acknowledgement.md)讲解每次投递如何结算。
