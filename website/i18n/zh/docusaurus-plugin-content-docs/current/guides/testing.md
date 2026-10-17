---
title: 测试 NATS 服务
sidebar_position: 8
description: "用 TestApp 在进程内测试 NATS 服务：生产环境的应用、流、回复、延迟，以及在 NATS 服务器上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 NATS 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 NATS](../components/testing/index.md) 或者服务器。

被测服务负责银行卡扣款。
付款到达 `card.payments`，由流 `CARD_PAYMENTS` 保存，一个持久化消费者读取它。
处理器在 `card.receipts` 上以一张收据作为回复。
没有银行卡的付款被丢弃。
超出银行即时限额的付款会被暂缓：处理器用 [`retry_after`](../components/settlement/retry-after.md) 要求一分钟后再投递它。
经过三次投递后，付款进入[死信 subject](../components/settlement/dead-letters.md) `card.review`。
应用由 `service(broker)` 构建，所以测试可以选择 Broker 的地址，而 `main` 使用生产环境的地址。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-nats` 的 `testing` feature。

## 付款及其收据 {/* #a-payment-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`NatsBroker`](../components/broker/index.md) 连接到进程内 NATS。
测试从不连接这个地址，也不需要事先创建任何流。
测试按生产类型访问 Broker：`tb.broker::<NatsBroker>()`。
`message(..).publish()` 把付款发布到它的类型所声明的 subject，流保存它。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card.receipts`。
测试主体把已启动的应用作为参数接收，所以下面的服务器测试也运行它。

## 没有银行卡的付款 {/* #a-payment-without-a-card */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#declined"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#declined"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
一分钟的 `HOLD` 不花费任何时间。
消费者像服务器一样扣留这笔付款，并在延迟结束时再次投递它。
第三次投递是挂载点允许的最后一次，所以这笔付款按到达时的样子进入 `card.review`。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内 NATS 模拟了什么 {/* #what-the-in-process-nats-models */}

进程内 NATS 读取客户端会读取的内容：地址，以及会改变服务器投递内容的连接选项（`no_echo`、inbox 前缀）。
它像服务器一样路由和保存消息：

- subject 按 `*` 和 `>` 匹配，队列组把每条消息交给一个成员。
- 流维护序号、自己的去重窗口，以及发布所声明的预期条件。
- 消费者从投递策略指定的位置开始，并带有 [JetStream 上下文](../components/context/index.md)的元数据。
- 确认、带或不带延迟的否定确认以及终止，都与服务器上的行为一致；未结算的投递在 `ack_wait` 之后回来。
- 请求到达应用的响应方，无人读取的请求立即得到 "no responders"。

客户端或服务器拒绝的东西，它同样拒绝：带空白字符的 subject、没有 NATS 形式的消息头、超过 1 MiB 的消息、没有任何流接收的 JetStream 发布。
在那里，流保存应用的消费者所读取的消息，订阅能找到它指名的每个流。
流自身的 subject 列表和限制、`max_ack_pending`、认证和 TLS 需要服务器。

## 在服务器上运行同一个测试 {/* #the-same-test-on-a-server */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到正在运行的 NATS 服务器。
测试用 `live::url`（示例自带的辅助函数）从 `NATS_TEST_URL` 读取地址，未设置时跳过。
流是服务器的状态，所以测试先在自己的连接上创建 `CARD_PAYMENTS`，结束后再删除它。
删除流也会删除它的消费者，所以下一次运行从干净的状态开始。
断言与进程内的完全相同。

```bash
cargo test
NATS_TEST_URL=nats://localhost:4222 cargo test -- --test-threads=1
```
