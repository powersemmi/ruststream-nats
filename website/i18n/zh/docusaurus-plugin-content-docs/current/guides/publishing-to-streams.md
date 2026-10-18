---
title: 向流发布
sidebar_position: 5
description: "把回复和消息发布到 JetStream 流中，用消息 id 让重复的消息只保存一次，并拒绝基于过时版本的写入。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 向流发布

本指南把回复和消息发布到 JetStream 流中，让重复的消息只保存一次，并拒绝基于过时版本的写入。

普通发布经由 Core NATS 发出，没有任何一方确认它。
[`JetStreamPublish`](../components/publishing/jetstream-publish/index.md) 等待流确认每条消息。
因此，流拒绝的消息是处理器能看到的错误，而不是悄无声息的丢失。

示例是一个订单服务。
它确认每个订单，把订单归档，并保存每个客户账户的历史。
它写入的流已经在服务器上存在：

```bash
nats stream add ORDERS --subjects 'orders.placed' --defaults
nats stream add CONFIRMATIONS --subjects 'confirmations.orders' --defaults
nats stream add ARCHIVE --subjects 'archive.orders' --defaults
nats stream add ACCOUNTS --subjects 'accounts.*' --defaults
```

## 写入流的回复 {/* #a-reply-into-a-stream */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#confirm"
```

</TabItem>
</Tabs>

`confirm` 在 `confirmations.orders` 上用一个 `Confirmation` 回复每个订单。
下面的应用通过 `JetStreamPublish::default().expect_stream("CONFIRMATIONS")` 发送这个回复。
[`expect_stream`](../components/publishing/jetstream-publish/expect-stream.md) 指定必须保存该 subject 的流，会由另一个流接收的发布将被拒绝。
只有在流保存了订单的确认消息之后，订单才被确认。
确认消息被拒绝时，这次投递失败，订单会回来。

回复的 subject 不在 `ORDERS` 的 subject 之内。
保存到处理器所读的流中的回复，会再次到达这个处理器。

## 只保存一次 {/* #storing-a-message-once */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#archive"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#archive"
```

</TabItem>
</Tabs>

一个订单可能被投递两次，这时 `archive` 会把它的副本发布两次。
[`message_id`](../components/publishing/jetstream-publish/message-id.md) 让同一订单的每个副本带上相同的 id，它以 `Nats-Msg-Id` 消息头传递。
流保存第一个副本，对重复的副本只确认而不保存，所以 `index` 对每个订单只读到一次。
流在自己的去重窗口内记住这些 id，流未另行设置时为两分钟。

`message_id` 描述的是一条消息，所以它是发布的一个步骤，而不是挂载点的设置。
这些步骤来自本 crate 的 prelude 中的 [`JetStreamPublishSteps`](api:ruststream_nats::JetStreamPublishSteps)。
调用其中某个步骤的处理器主体，要把它的槽位限定为 `Out<impl Publisher<Options = JetStreamOptions>, _>`。
这个约束把处理器绑定到 JetStream 上，这也是处理器唯一一处指名 Broker 的地方。

## 拒绝过时的写入 {/* #refusing-a-stale-write */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#accounts"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#accounts"
```

</TabItem>
</Tabs>

账户 42 的历史就是流中的 subject `accounts.42`。
客户端基于它最后读到的版本修改账户的电子邮件地址，这个版本就是账户最后一个事件的流序号。
[`expect_last_subject_sequence`](../components/publishing/jetstream-publish/expectations.md) 让流只在该 subject 的最后一条消息仍处于这个序号时才保存事件。
读到同一版本的两个客户端不能都写入：第二次写入被拒绝，`change_email` 丢弃它。
`expect_last_sequence` 检查的是整个流而不是一个 subject，`expect_last_message_id` 检查流中最后一条消息的 id。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#app"
```

</TabItem>
</Tabs>

每次 JetStream 发布都等待流的确认。
在投递之外，启动钩子接收该策略，[`publish_ack`](../components/publishing/jetstream-publish/publish-ack.md) 返回这个确认：流、序号，以及去重窗口是否已经见过这条消息。
在可以接受丢失消息的地方，Core NATS 发布是合适的选择，比如[确认与投递保证](acknowledgement.md)中的看板。

示例文件中的测试在进程内运行应用：进程内的流像服务器一样检查消息 id 和预期条件。
