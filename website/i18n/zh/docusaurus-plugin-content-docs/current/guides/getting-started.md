---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-nats，启动本地 NATS 服务器，让第一个订阅者连上它运行，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-nats`，启动一个本地 NATS 服务器，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-nats` 是 NATS Broker。
它的 `testing` feature 加入测试所用的[进程内 NATS](../components/testing/index.md)。

这个 Broker crate 有两个 feature，默认都不开启：

| Feature | 提供的内容 |
|---|---|
| `testing` | `TestApp` 背后的进程内 NATS |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [NATS 绑定](../components/asyncapi/index.md) |

底层客户端是 [async-nats](https://docs.rs/async-nats)。
它用 Rust 编写，通过 rustls 实现 TLS，所以构建不需要任何系统库。

## 本地 NATS {/* #a-local-nats */}

开发时，一个开启了 JetStream 的 NATS 服务器就够了：

```yaml title="compose.yaml"
services:
  nats:
    image: nats:2-alpine
    command: ["-js"]
    ports:
      - "4222:4222"
```

```bash
docker compose up -d
```

服务器监听 `localhost:4222`。
`-js` 开启 JetStream，即从[流与消费者](streams-and-consumers.md)开始的各篇指南所读写的流。
示例用 [NATS 命令行工具](https://github.com/nats-io/natscli) `nats` 发布消息。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 Core NATS subject `orders.created` 读取 JSON 订单。
它接受带金额的订单，丢弃没有金额的订单。
[`NatsBroker::new`](../components/broker/index.md) 接收服务器地址，只把它记录下来。
连接在服务启动时才打开。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里发布一个订单：

```bash
nats pub orders.created '{"id":1,"amount_cents":1250}'
```

服务记录一条带订单 id 的 `order taken` 日志。
Core NATS 没有确认机制，所以日志还会为每个订单显示 `ack / nack failed`：运行时报告这次投递无法结算。
[确认与投递保证](acknowledgement.md)说明这在什么时候要紧，以及流如何结算每条消息。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

`cargo generate` 可以生成同类服务的脚手架，其中带有路由器：

```bash
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats --name my-service
```

`templates/nats-js` 生成的服务通过持久化消费者读取一个流。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`NatsBroker` 连接的是进程内 NATS，所以测试不需要服务器：

```bash
cargo test
```

[测试 NATS 服务](testing.md)逐一讲解这些测试，以及进程内 NATS 模拟了什么。

## 从 async-nats 迁移 {/* #coming-from-async-nats */}

基于 async-nats 的服务要自己编写订阅循环，并手动发布消息。
下表列出每一部分在 RustStream 中的位置：

| async-nats | RustStream |
|---|---|
| `async_nats::connect` 或 `ConnectOptions::connect` | [`NatsBroker::new`](../components/broker/index.md)，`ConnectOptions` 用 [`with_options`](../components/broker/connect-options.md) 传入 |
| `client.subscribe` 加一个 `while let Some(message)` 循环 | 一个 `#[subscriber("subject")]` 处理器 |
| `client.queue_subscribe` | [`CoreSubject::queue_group`](../components/subscriptions/queue-group.md)（[Subject、通配符与队列组](subjects-and-queue-groups.md)） |
| `serde_json::from_slice(&message.payload)` | 处理器的参数类型，由编解码器解码 |
| `client.publish` | 回复或 `Out` 参数，经由 [`Publish`](../components/publishing/index.md) 策略发送 |
| `client.request` | `Out<impl RequestReply, _>` 槽位上的 [`request`](../components/request-reply/index.md)（[请求-应答](request-reply.md)） |
| 在流上调用 `create_consumer` 加一个 `messages()` 循环 | [`JetStreamSubject`](../components/subscriptions/jetstream-subject.md)（[流与消费者](streams-and-consumers.md)） |
| `message.ack()`、`AckKind::Nak`、`AckKind::Term` | 处理器的结果：`ack`、`retry`、`retry_after`、`drop`（[确认与投递保证](acknowledgement.md)） |
| `jetstream.publish` 及其确认 | [`JetStreamPublish`](../components/publishing/jetstream-publish/index.md)（[向流发布](publishing-to-streams.md)） |
