---
title: 确认与投递保证
sidebar_position: 4
description: "用处理器的结果结算 JetStream 投递，为慢处理器设定 ack_wait，处理重新投递，并了解 Core NATS 不保证什么。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 确认与投递保证

本指南用处理器的结果结算 JetStream 投递，为慢处理器设定 `ack_wait`，处理投递了两次的消息，并说明 Core NATS 不保证什么。

处理器用它返回的值结算自己的投递，运行时在处理器结束后发送结算。
在 JetStream 消费者上，每种结果都是一个服务器据以行动的确认：

| 结果 | 发给服务器 | 服务器做什么 |
|---|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | `+ACK` | 这个消费者不再保留这条消息 |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | `-NAK` | 立即再次投递它 |
| [`retry_after(delay)`](../components/settlement/retry-after.md) | 带延迟的 `-NAK` | 扣留它，延迟过后再次投递 |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | `+TERM` | 不再投递它 |
| 没有结算 | 什么也不发 | [`ack_wait`](../components/settlement/ack-wait.md) 过后再次投递它 |

重新投递保留消息的流序号，服务器统计它的投递次数。

示例是一个支付服务。
它通过银行扣收已授权的付款，并把每次扣款通知给看板。

## 结算一笔付款 {/* #settling-a-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#capture"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#capture"
```

</TabItem>
</Tabs>

银行有四种答复，每种需要不同的结果。
扣款成功时返回 `Captured`，运行时把它作为回复发布，然后确认这笔付款。
`Busy` 表示银行的速率限额已经用完，所以付款在服务器上等待 30 秒。
`Unavailable` 表示银行没有应答，所以付款立即回来。
`Declined` 表示这张卡无法扣款，再试一次也是同样的答复，所以这笔付款被终止。
不设上限的 `retry` 会无休止地回来；[重试与死信](retries.md)加上上限和死信 subject。

## 投递了两次的消息 {/* #messages-delivered-twice */}

JetStream 至少投递一次。
在 `retry` 之后、`ack_wait` 耗尽之后，以及进程在结算前停止之后，付款都会回来。
所以处理器会两次看到某些付款，它的工作必须能接受重复。
这里银行把付款 id 当作幂等键，重复的扣款只扣一次。
[`Ctx<Delivered>`](../components/context/index.md) 是服务器统计的投递次数，第一次投递时为 1；`capture` 会记录它再次看到的付款。

## 慢处理器与 `ack_wait` {/* #slow-handlers-and-ack_wait */}

服务器等待结算的时间是 [`ack_wait`](../components/settlement/ack-wait.md)，描述符未另行指定时为 30 秒。
超时后它再次投递消息，而第一个处理器可能仍在运行。
`capture` 设为 60 秒，高于银行答复的最长时间。
把 `ack_wait` 设得高于处理器的最坏耗时，两次投递就永远不会重叠。
耗尽的 `ack_wait` 也计入[尝试上限](retries.md)的投递次数。

## Core NATS 上的看板 {/* #the-dashboard-on-core-nats */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#dashboard"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#dashboard"
```

</TabItem>
</Tabs>

`refresh` 从一个 Core NATS subject 读取扣款。
[Core NATS](../components/settlement/at-most-once.md) 至多投递一次，并且没有确认。
结算 Core 投递会报告 [`AckError::Unsupported`](api:ruststream::AckError::Unsupported)，运行时把它记录为警告。
进程丢失的 Core 消息就此消失，没有任何东西能把它找回来。
看板可以丢失一次更新，因为下一次扣款会再次刷新它。
付款不能丢失，所以付款经由流传递。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

银行客户端是应用的状态，在启动时打开一次。
批处理器用一个结果结算整批；返回 `Vec<HandlerOutcome>` 时，则逐条结算每条消息。

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `Busy` 的 30 秒瞬间就过去了。
