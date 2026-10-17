---
title: Subject、通配符与队列组
sidebar_position: 2
description: "为 Core NATS subject 命名，用通配符读取多个 subject，在队列组中分担工作，并在并行的工作分区中保持每个键的顺序。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subject、通配符与队列组

本指南为 Core NATS subject 命名，用通配符读取多个 subject，在队列组中让各个副本分担工作，并在并行的工作分区中保持每个键的顺序。

subject 是由点分隔的一串 token，比如 `orders.eu.created`。
Core NATS 把消息交给此刻 subject 与之匹配的每个订阅，并且什么也不存储。

示例是一个订单服务。
它打包欧洲的订单，为订单开账单，并审计每个地区的每个订单事件。

## 队列组 {/* #a-queue-group */}

`pack` 读取 `orders.eu.created`。
服务运行多个副本，而每个订单只能打包一次。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#pack"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#pack"
```

</TabItem>
</Tabs>

[`CoreSubject`](../components/subscriptions/core-subject.md) 读取一个 subject。
[`queue_group`](../components/subscriptions/queue-group.md) 把订阅放进 `packers` 组。
服务器把每条消息交给组内的一个成员，所以每个副本打包一部分订单。
对服务器来说，一个组名就是一个组，即使成员订阅的 subject 不同。
组外的订阅，或者另一个组里的订阅，仍然收到属于自己的一份。

## 按客户划分的并行工作分区 {/* #parallel-lanes-per-customer */}

`bill` 读取同一个 subject，但不加入任何组，所以 `pack` 收到的每个订单它都会收到。
它最多同时为八个订单开账单，并保持每个客户的顺序。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#bill"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#bill"
```

</TabItem>
</Tabs>

`workers(8, by_key)` 运行八个工作分区。
[键](../components/subscriptions/keys.md)相同的投递在同一个工作分区上依次运行。
在手写形式中，同一设置是 [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key)。
NATS 没有原生的键，所以键以 `nats-partition-key` 消息头（[`PARTITION_KEY_HEADER`](api:ruststream_nats::PARTITION_KEY_HEADER)）传递，由发送方设置。
`CustomerKey` 是一个消息头契约，只有一个以该名字命名的字段。
`Headers<CustomerKey>` 读取投递的键，`with_headers(&key)` 把它写到账单上。
于是对下一个读者来说，同一客户的账单也保持顺序。
没有该消息头的订单不满足消息头契约，由解码策略结算，默认会丢弃它。

NATS 消息头是文本：名字是不含冒号的可打印 ASCII 字符，值只有一行。
不符合这些要求的消息头会让发布返回错误。

## 通配符 {/* #a-wildcard */}

`audit` 读取每个地区的每个订单事件。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#audit"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#audit"
```

</TabItem>
</Tabs>

[`CoreWildcard`](../components/subscriptions/core-wildcard.md) 读取模式匹配的每个 subject。
`*` 匹配一个 token：`orders.*.created` 读取每个地区的订单创建事件。
`>` 匹配末尾的一个或多个 token：`orders.>` 读取 `orders` 之下的每个 subject。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#app"
```

</TabItem>
</Tabs>

模式读取多个 subject，所以它的投递的延迟副本没有唯一的 subject 可去。
挂载点用 [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md) 指定一个，这里是 `orders.retry`，模式同样读取它。
每个基于模式的注册都要指定它，即使处理器从不重试；没有它，服务拒绝启动。
[重试与死信](retries.md)讲解这些副本。

## 至多一次 {/* #at-most-once */}

Core NATS 把消息投递给它到达时已经存在的订阅。
在没有任何订阅匹配时发布的消息就此消失。
投递也没有确认，所以处理器失败后，服务器不会再次交给它这条消息。
如果每条消息都不能丢，就让 subject 归属一个流：[流与消费者](streams-and-consumers.md)展示如何读取流。

示例文件中的测试在进程内运行应用，检查每个订阅收到的那一份、模式，以及每个客户的账单顺序。
