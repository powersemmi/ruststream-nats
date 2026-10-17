---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 NATS 上变成什么：在 JetStream 上是服务器据以行动的确认，在 Core 上是运行时发布的副本。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "处理器在 JetStream 投递上的每种结果：投递发送什么，服务器做什么",
  "text": "以文字阅读本图",
  "outcomeLane": "处理器",
  "wireLane": "发送",
  "serverLane": "服务器",
  "ack": ["ack()"],
  "retry": ["retry()"],
  "retryAfter": ["retry_after(d)"],
  "drop": ["drop()"],
  "unsettled": ["ack_wait 内", "没有结果"],
  "nothing": "什么也不发",
  "ackEffect": ["完成，", "不再发送"],
  "retryEffect": ["立即再次发送，", "delivered + 1"],
  "retryAfterEffect": ["扣留 d，", "然后再次发送，", "delivered + 1"],
  "dropEffect": ["结束，", "不再发送"],
  "unsettledEffect": ["ack_wait 之后", "再次发送，", "delivered + 1"],
  "core": ["在 Core NATS 上，每种结果都报告 AckError::Unsupported：", "服务器不结算任何东西"]
};

export const retries = {
  "title": "NATS 上的一次重试：尝试上限决定是把消息带回来，还是发往死信 subject",
  "text": "以文字阅读本图",
  "delivery": ["一次投递", "JetStream 统计投递次数；Core 靠消息头传递"],
  "outcome": ["处理器返回", "retry_after(delay)，或在有声明时返回 retry()"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "below": "低于上限",
  "nak": ["JetStream：", "带延迟的 -NAK，", "由服务器扣留"],
  "copy": ["Core subject：", "副本发布到", "该 subject"],
  "named": ["Core 模式：", "副本发往", "挂载点指定的", "subject"],
  "atCap": "到达上限",
  "deadLetter": ["死信 subject：", "投递按到达时", "的原样", "发布"],
  "terminated": ["没有死信：", "JetStream 上 +TERM，", "Core 上", "不再处理"],
  "lost": ["Core 副本在进程内等待它的延迟：", "计时器触发前停止就会丢失它"],
  "handled": "已处理",
  "retries": "重试",
  "belowCap": "低于上限",
  "reached": "到达上限"
};

# 结算

`ack`、`drop`、`retry` 和 [`retry_after`](../components/settlement/retry-after.md) 在 NATS 上变成什么：在 JetStream 上是服务器据以行动的确认，在 Core 上是运行时发布的副本。

<Diagram name="settlement" words={table} />

JetStream 投递在服务器上结算。应答发往该投递的确认 subject，消费者按[消费者](nats/consumers.md)一页图中所示的方式处理它。`retry_after` 把延迟直接放进否定确认里。服务器扣留这条消息，然后在同一个消费者上再次投递它，流序号不变，`delivered` 计数加一。不发布任何东西，也不生成副本。

Core 投递无需结算。`ack`、`retry` 和 `drop` 报告 `AckError::Unsupported`，运行时记录日志。服务器不保存任何可以再次投递的东西，运行时只能以它发布的副本把消息带回来。这就是[至多一次](../components/settlement/at-most-once.md)投递。

| 结果 | Core NATS |
|---|---|
| `ack`、`drop` | `AckError::Unsupported` |
| `retry()` | `AckError::Unsupported`；声明了上限时，立即发布一个副本 |
| `retry_after(d)` | 在 `d` 之后发布一个副本 |
| 没有结果 | 什么也不发生 |

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

上限在挂载点用 [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 声明，第一次投递算作一次。在 JetStream 上，次数就是服务器自己的 `delivered`，因此超过 `ack_wait` 的投递也计算在内。在 Core 上，运行时统计它发布的副本，次数记在副本的 `x-ruststream-retry-count` 消息头里。

低于上限时，两种模型各自把消息带回来。JetStream 从流中重新投递它。在 Core 上，运行时把副本发布到描述符报告的地址。[`CoreSubject`](../components/subscriptions/core-subject.md) 报告它自己的 subject。[`CoreWildcard`](../components/subscriptions/core-wildcard.md) 匹配许多 subject，却不向任何一个发布，所以它的注册要[指定副本发往的 subject](../components/settlement/out-retry.md)，否则应用拒绝启动。

到达上限时，投递按到达时的样子发往[死信 subject](../components/settlement/dead-letters.md)，载荷和消息头都不变。没有声明死信 subject 时，JetStream 投递以 `+TERM` 终止，Core 投递则就此放过。

本 crate 不向服务器声明任何东西：`max_deliver` 保持未设置，因为 JetStream 没有地方可以发送投递次数耗尽的消息。运行时的路径覆盖两种模型。
