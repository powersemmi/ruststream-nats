---
title: 重试与死信
sidebar_position: 6
description: "立即或稍后重试一次 NATS 投递，限定尝试次数，并把一直失败的消息存入死信流，适用于流、subject 和模式。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信

本指南立即或稍后重试一次投递，限定尝试次数，并把一直失败的消息存入[死信](../components/settlement/dead-letters.md)流，适用于流消费者、Core subject 和模式。

NATS 没有死信机制。
JetStream 可以限定一条消息的投递次数，但不会把最后一次投递发往任何地方。
因此由挂载点声明上限和死信 subject，框架把它们应用到两种模型上。

示例是一个支付服务，它通过银行结清付款并发出退款。
它把死信保存在一个专用的流中：

```bash
nats stream add PAYMENTS --subjects 'payments.settle' --defaults
nats stream add DEAD --subjects 'dead.*' --defaults
```

## 在流上重试 {/* #retrying-on-a-stream */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#settle"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#settle"
```

</TabItem>
</Tabs>

`outcome` 把银行的拒绝变成处理器的结果。
银行繁忙时，[`retry_after`](../components/settlement/retry-after.md) 在 30 秒后再问一次。
在 JetStream 消费者上，延迟放在否定确认里：服务器扣留这笔付款，然后在同一个消费者上以相同的流序号再次投递它。
不发布任何东西，在此期间停止的进程也不会丢失任何东西。
银行没有应答时，`retry` 立即再问一次；卡被拒时，付款被丢弃。

## 上限与死信 {/* #the-cap-and-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) 统计一笔付款的每次投递，第一次也算在内。
在 JetStream 消费者上，这个次数就是服务器自己统计的次数，所以耗尽的 [`ack_wait`](../components/settlement/ack-wait.md) 也计算在内。
第五次投递不再重试：[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 把它发布到 `dead.payments`，载荷和消息头保持到达时的样子。
被丢弃的付款完成结算，不会去任何地方，所以死信中只有用完了尝试次数的付款。
设了上限却没有死信 subject 的消费者，会终止它的最后一次投递。

`dead.payments` 是一次普通发布，而 Core NATS 不保存无人读取的消息。
`DEAD` 流保存它，所以死信会一直等到有人查看。
应用读取的任何 subject 都不与死信 subject 匹配，否则用完尝试次数的投递会回到放弃了它的处理器。

## 读取死信 {/* #reading-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` 像读取其他流一样从 `DEAD` 流读取失败的付款，并把每一笔记入日志，留给人来查看。

## 在 Core subject 上重试 {/* #retrying-on-core-subjects */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refunds"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refunds"
```

</TabItem>
</Tabs>

Core NATS 无法扣留消息。
在 Core 订阅上，一次重试就是框架发布的投递副本：`retry` 立即发布它，`retry_after` 在延迟结束时发布它。
副本在 [`x-ruststream-retry-count`](api:ruststream::runtime::RETRY_COUNT_HEADER) 消息头中带着投递次数，上限读取的就是它。
延迟的副本是至多一次的：在延迟结束前停止的进程会丢失它。

[`CoreSubject`](../components/subscriptions/core-subject.md) 把副本发布到它自己的 subject。
`send_refund` 读取模式 `refunds.*`，这个模式没有唯一的 subject 可供发布。
所以应用用 [`out_retry(Publish).to("refunds.retry")`](../components/settlement/out-retry.md) 指定一个模式能匹配的 subject，副本就会回到 `send_refund`。
基于模式却没有这一设置的注册拒绝启动，不论它的处理器是否重试。
退款的第三次投递进入 `dead.refunds`，次数记在消息头里。

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
[测试 NATS 服务](testing.md)讲解了测试套件。
