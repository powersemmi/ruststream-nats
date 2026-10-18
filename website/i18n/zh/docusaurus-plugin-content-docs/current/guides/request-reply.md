---
title: 请求-应答
sidebar_position: 7
description: "通过 NATS 向另一个服务发出请求并等待应答，在请求的 inbox 上应答，处理超时和 no responders，并扩展响应方。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 请求-应答

本指南通过 NATS 向另一个服务发出请求并等待应答，在请求的 inbox 上应答，处理无人应答的请求，并扩展应答一方。

NATS 自己把应答与请求对应起来。
请求带有一个 inbox，即它专属的 subject，应答就发布到这个 inbox。

示例是同一个应用中的两个服务。
定价服务回答价格查询，订单服务向它询问每个订单的价格。

## 响应方 {/* #the-responder */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

请求到达时，它的 inbox 在 `reply-to` 消息头里，`ctx.headers().reply_to()` 读取它。
`quote` 用 [`to(inbox)`](../components/request-reply/responders.md) 通过它的 `Answers` 槽位发布价格，因为目的地要到每个请求时才知道。
因此 `Price` 不声明自己的 subject。
没有价格的商品得不到应答，请求方的超时结束等待。

响应方在队列组 `pricing` 中读取 `prices.quote`。
定价服务的每个副本都加入这个组，每个请求交给其中一个副本。
任何 NATS 响应方都能应答同一个请求，比如在命令行中运行 `nats reply prices.quote '{"sku":"book","cents":1250}'`。

## 请求方 {/* #the-requester */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#requester"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#requester"
```

</TabItem>
</Tabs>

`price_order` 从一个流读取新订单，并在 `prices.quote` 上询问每个订单的价格。
`Pricing` 槽位的约束是 `Out<impl RequestReply, _>`：主体说明它需要[请求-应答](../components/request-reply/index.md)，但不指名任何 Broker。
应用把这个槽位与普通的 `Publish` 策略配对，在 NATS 上，该策略的发布者具备请求-应答能力。
[`request`](api:ruststream::RequestReply::request) 用一个新的 inbox 发布问题，并以其上的第一条消息作为结果。
问题是一条原始消息，所以主体自己编码问题、解码应答，这里用的是 `serde_json`。
价格确定后，订单的回复 `Priced` 随即发出。

请求有两种失败方式。
没有任何订阅读取该 subject 时，服务器立即以 "no responders" 应答。
响应方存在却一直沉默时，请求在超时后返回 [`NatsError::RequestTimeout`](api:ruststream_nats::NatsError::RequestTimeout) 错误，这里的超时是两秒。
无论哪种情况，订单都在流上等待 30 秒，然后再问一次。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#app"
```

</TabItem>
</Tabs>

两个槽位都使用普通的 `Publish` 策略：一个发布应答，另一个提出问题。
启动钩子也以同样的方式提问：调用 `b.after_startup(Publish, ..)`，在交给它的发布者上调用 `request`。

示例文件中的测试在进程内的一个应用中运行这两部分。
进程内 NATS 把请求路由到应用自己的响应方，在没有订阅读取该 subject 时应答 "no responders"，并让沉默的响应方超时。
