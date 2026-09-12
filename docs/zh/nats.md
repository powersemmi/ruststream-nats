# NATS { #nats }

`ruststream-nats` 让 RustStream 服务运行在 NATS 上。NATS 有两种投递模型，这个 crate 两种都覆盖。
Core NATS 把消息交给此刻订阅着的订阅者，自己什么都不保留。JetStream 把它存进流里：那是一份日志，
和 Kafka 的一样。`testing` feature 增加一个进程内的 NATS 传输，因此你不需要服务器也能测试服务。
框架本身的概念（怎样写订阅者、路由、编解码器和中间件）参见
[RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

## 一个文件写哪个 glob { #which-glob-a-file-writes }

处理器文件写 `ruststream::prelude::*`，并用它需要的那项能力约束注入进来的发布者
（`Out<impl Publisher>`、`Out<impl RequestReply>`）。这样的主体不提任何 Broker，因此同一个处理器
原样挂载到真实服务器上，也原样挂载到进程内传输上。

路由文件写 `ruststream_nats::prelude::*`。这个 crate 的 prelude 重导出框架的那一个，再加上本
crate 的 Broker、它的两个订阅描述符和它的发布策略。它们在挂载点的名字在每个 Broker 上都一样：
`Publish` 就是这个文件挂载到的传输上的普通发布。

单文件的服务两者都是，因此它取 Broker 的 prelude。

还有一种处理器主体也取 Broker 的 prelude：设置单条消息的 JetStream 选项的那一种。参见
[一条 JetStream 消息对自己的声明](#what-one-jetstream-message-states-about-itself)。

## 生命周期 { #the-lifecycle }

三个类型，连接的每个状态各一个：

```text
NatsBroker::new(url)      只有配置，同步，没有 I/O
  .connect()   ->  ConnectedNatsBroker     活动的连接；订阅和发布者
  .shutdown()  ->  ClosedNatsBroker        终态见证值，带着排空时的计数
```

`shutdown` 消费已连接的 Broker，因此写在它之后的发布或订阅无法通过编译。先前发出的发布者共用那条
连接，连接一旦不在，通过它的每次发布都返回 `NatsError::Closed`。

凭据、TLS 和其余的客户端调优都是 `async_nats::ConnectOptions` 的设置，`NatsBroker::with_options`
把它们附到 Broker 上。在框架之外建好的客户端，用 `ConnectedNatsBroker::from_client` 变成一个已
连接的 Broker。

## Core 订阅 { #core-subscription }

`#[subscriber("subject")]` 处理器绑定到一个 NATS subject：

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:handler"
```

把它挂载在 `with_broker` 里面：

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## JetStream 持久化消费者 { #jetstream-durable-consumer }

要改为从 JetStream 消费，就在 `#[subscriber(..)]` 属性里用 `JetStreamSubject` 写出来源：要读的
流，以及一个位置能跨重启保留的持久化消费者。

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:handler"
```

定义里已经写着自己的来源，因此挂载就是一句普通的 `include`：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:mount"
```

`nats-js` 这个 CLI 脚手架生成的正是这一对。

除了 `durable`，`JetStreamSubject` 上还有 `filter_subject`、`ack_wait`、`max_ack_pending`、
`deliver_policy` 和 `pull_expires`（一次 pull 请求最多等多久，然后带着已经到手的消息返回）。Core
NATS 的负载均衡是 `CoreSubject::queue_group`。两个类型没有共同的设置，因此写在错误模型上的设置
无法通过编译。

`JetStreamSubject` 本身就是一个订阅来源，所以不用宏的那条路径原样接受它：
`subscriber(JetStreamSubject::new("orders.*", "ORDERS"), body)` 构造出同一个定义。那条路径上主体
的契约，参见框架文档里的
[订阅者](https://powersemmi.github.io/ruststream/latest/guides/subscribers/)。

### 批 { #batches }

接收 `&[T]` 的处理器消费一个批：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:batch"
```

挂载点补上批的大小：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:batch_mount"
```

在 JetStream 上，这个数字就是 pull 请求的批大小：一个批是一次 `fetch`，最多六条消息；时限内到得
更少时，`pull_expires` 提前结束它。Core NATS 的协议里没有批，因此框架的 `Buffered` 适配器在客户端
把批攒出来，不满的批在第一次投递之后 10 ms 关闭。

这个大小属于注册，不属于订阅。因此 `JetStreamSubject` 上有的是时间（`pull_expires`），不是条数。

### 确认与延迟重试 { #acknowledgement-and-delayed-retry }

一次 JetStream 投递在服务器上结算：`HandlerOutcome::ack()` ack 它，`HandlerOutcome::retry()` 发出
一次否定确认，`HandlerOutcome::drop()` 终止它。`HandlerOutcome::retry_after(delay)` 把延迟放进否定
确认本身，于是服务器把这条消息押住这么久，再在同一个消费者上重新投递它，它的流序号和投递次数都
保持原样。

Core NATS 根本没有确认：一次 core 投递返回 `AckError::Unsupported`，那里的 `retry_after` 退回到
运行时的延迟重新发布。这条退路需要一个地方来送那份副本，而订阅回答的是它读取的 subject：Core
订阅回答 subject 本身，JetStream 订阅回答消费者的过滤条件。因此 `BrokerScope::retry_via` 在这个
Broker 上不用额外接线就能用。

通配符是例外。`orders.*` 在投递时匹配，在发布时服务器拒绝它，因此按模式打开的订阅报不出地址；在
这样的订阅之上用 `retry_via` 接线的作用域会拒绝启动，而不是把副本发往不存在的地方。给这样的处理器
一个具体的 subject，或者改用 JetStream 读它：在那里 `retry_after` 就是服务器自己的延迟否定确认。

## 发布 { #publishing }

你写下哪个发布策略，就选定了哪种传输：

- `NatsPublish` 构造 `NatsPublisher`：Core NATS 的普通发布，发完即忘，同一个活值上还带
  `RequestReply` 能力。它也是这个 Broker 默认的发布策略，因此没有写 `.out(Reply, ..)` 的应答处理器
  就通过它回复。crate 的 prelude 把它叫作 `Publish`。
- `JetStreamPublish` 构造 `JetStreamPublisher`：每次发布都等流的确认，因此流拒绝掉的消息会返回
  错误，而不是悄悄丢掉。`publish_ack` 返回确认本身：流、序号，以及去重窗口是否认出了这条消息。
  这个策略还用 `expect_stream` 写明这个 subject 必须由哪个流来承载，路由到别处的发布它一律拒绝。

挂在 JetStream 消费者上的处理器，用返回值来回复：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:reply"
```

挂载点写出策略，因此一个处理器发出的确认消息进入一个会 ack 它们的流，而服务的其余部分继续通过
Core NATS 发布：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:reply_mount"
```

在处理器之外，同一个策略在启动时实例化出发布者：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:publish"
```

### 一条 JetStream 消息对自己的声明 { #what-one-jetstream-message-states-about-itself }

一个去重 id 和一个在流中的期望位置，描述的是一条消息，不是一个发布者，因此它们随这次发布一起
传递。保存它们的值是 `JetStreamOptions`，由发布构建器写入：

| 步骤 | 协议字段 | 服务器拿它做什么 |
| --- | --- | --- |
| `message_id(id)` | `Nats-Msg-Id` | 在去重窗口内，重复的那一条只存一次。 |
| `expect_last_sequence(n)` | `Nats-Expected-Last-Sequence` | 流不在 `n` 上就拒绝这次发布。 |
| `expect_last_subject_sequence(n)` | `Nats-Expected-Last-Subject-Sequence` | 同一件检查，针对这条消息自己的 subject。 |
| `expect_last_message_id(id)` | `Nats-Expected-Last-Msg-Id` | 流里最后一个 `Nats-Msg-Id` 不是 `id` 就拒绝这次发布。 |

主体在发布构建器上调用一个步骤：

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:options"
```

这是处理器主体唯一一处提到 Broker 的地方。步骤来自 `ruststream_nats::prelude`，槽位约束成
`Out<impl Publisher<Options = JetStreamOptions>, Archive>`，因此签名直说这个主体是为 JetStream
写的。不调用步骤的主体保留框架的 prelude，挂载到任何 Broker 上。

挂载点仍然写出策略，两者不重叠：发布者写进哪个流，由挂载点说了算；一条消息声称流处于什么状态，
由主体说了算。

```rust
--8<-- "crates/ruststream-nats/examples/nats_jetstream.rs:options_mount"
```

步骤是构建器上的一个位置，不是发布者外面的一层包装，因此它结束的那次发布照样走挂载点自己的那个
条目出去，用的是那个条目写明的编解码器。

Core NATS 没有自己的单条消息设置：一条 core 消息就是 subject、载荷和消息头，因此 `NatsPublisher`
声明 `Options = ()`，上面那些步骤在它之上的构建器里不在作用域内。

## 请求-响应 { #request-reply }

NATS 原生就把回复对应起来，因此 `NatsPublisher` 实现了 `RequestReply` 能力，crate 的 prelude 也把
它重导出。`request(msg, timeout)` 发布消息时带上一个回复地址，返回回复消息；时限内没人应答，就
返回超时错误：

```rust
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_nats::prelude::*;

--8<-- "crates/ruststream-nats/examples/nats_request_reply.rs:request"
```

任何 NATS 应答方都能回答它：另一个服务，或者命令行里的 `nats reply questions 'pong'`。可运行的
程序是
[`examples/nats_request_reply.rs`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_request_reply.rs)。

进来的请求把自己的回复地址放在众所周知的 `reply-to` 消息头里，因此应答方读
`ctx.headers().reply_to()`，通过注入的发布者把答复发布到那个 subject。

## 各项能力 { #capabilities }

框架的可选能力 trait 里，这个 Broker 原生实现了哪些：

| 能力 | 原生 | 说明 |
| --- | --- | --- |
| `Subscribe` | 是 | 通过 `CoreSubject` 按 subject 订阅；`JetStreamSubject` 改为通过 JetStream 消费者来读它。 |
| `BatchSubscriber` | 是 | 在 JetStream 上，一个批是一次 pull `fetch`，最多取到挂载点写的 `batch(n)`，并受 `pull_expires` 限制。Core NATS 的协议里没有批，因此框架的 `Buffered` 适配器在客户端把批攒出来。参见[批](#batches)。 |
| `TransactionalPublisher` | 否 | 两种模型都没有跨多条消息的事务；JetStream 的发布一条一条确认。 |
| `OwnedTransactions` | 否 | 同样的原因：没有事务可以拥有。 |
| `RequestReply` | 是 | `NatsPublisher` 发布时带上原生的回复地址，并把回复返回。参见[请求-响应](#request-reply)。 |
| `Partitioned` | 是 | NATS 没有原生的分区，因此发送方把键写进 `nats-partition-key` 消息头，运行时 `workers(n, by_key)` 的各个分区从那里读它。 |
| `Seekable` + `Positioned` | 否 | `deliver_policy` 决定新建的 JetStream 消费者从哪里开始；活动的订阅不重新定位。 |
| `DescribeServer` | 是 | 报告配置里的地址，AsyncAPI 文档记下的就是它。 |

## 测试 { #testing }

`testing` feature 带来 `NatsTestBroker`：一个进程内传输，有真正的 NATS subject 匹配（`*` 和 `>`
通配符）、消息头传递和请求-响应，不需要 `nats-server`，也不需要 docker。它驱动 `TestApp` 测试
套件：用服务发布时的同一个构建器发布输入，套件报告处理器收到了什么、发布了什么，以及这次投递
怎样结算。参见
[用 `TestApp` 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

有六件 NATS 特有的事在进程内照样成立，因此用到它们的处理器不需要服务器也能测：

- `JetStreamSubject` 来源在这里同样解析得出；路由由 subject 模式决定。
- 竞争消费者确实竞争。Core 的 `queue_group`，以及共用一个 `JetStream` `durable` 的那些订阅，轮流
  各取一条消息，而不是各拿一份副本，正如服务器把消息交给这一组里的某一个成员；这样的集合之外的
  订阅，仍然收到每一条匹配的消息。没有这一条，测试会让两个 worker 做同一件活还算通过，所以这里
  把它复刻出来，而不是在文档里说明一下了事。这里的轮转是确定的，因此测试可以断言是哪个成员运行
  了；服务器自己挑成员，两者共同的性质是这一组对每条消息只看见一次。
- 发布策略就是生产用的那两个。`NatsPublish` 和 `JetStreamPublish` 在测试 Broker 上同样构造出
  发布者，`NatsPublish` 也是它的默认策略，因此路由文件原样挂载：
  `b.include(confirm).out(Reply, Publish)` 和
  `b.include(audit).out(Audit, JetStreamPublish::default())` 在两个 Broker 上说的是同一件事。没有
  测试传输自己的策略需要换进来。每个活值带的能力和它的生产对应物完全一样 - Core 是 `Publisher`
  和 `RequestReply`，`JetStream` 只有 `Publisher` - 因此在这里能编译的槽位，对着服务器也能编译。
- 一条消息对自己的声明会到达。步骤在这里写的是同一个 `JetStreamOptions`，传输把它变成真实客户端
  写的那些协议消息头，因此测试从任意一侧读都行：

    ```rust
    --8<-- "crates/ruststream-nats/tests/handlers.rs:options_assert"
    ```

    `assert_options_default()` 是对应的反向断言，用于没有调用任何步骤的发布。

- 用 `ruststream_nats::context` 的键绑定 `JetStream` 原生元数据的处理器可以挂载，每个键都读到
  `None`，和在一次 core 投递上完全一样。
- `HandlerOutcome::retry_after(delay)` 变成一次延迟重新投递，计时器由测试套件掌管，因此
  `tb.advance(delay)` 在暂停的时钟下触发它。

`JetStream` 本身的语义（持久化消费者的游标和它的恢复、`ack_wait` 的重新投递、保留策略，以及元数据
和服务端延迟真正做的事）不做模拟；这些要对着真实服务器测试，用 `NATS_TEST_URL` 开启。共用一个
durable 复刻出来的是它的订阅彼此竞争，而不是消费者上次读到了哪里。发布这一侧排除在外的是流：
进程内传输只是一套 Core 的 subject 匹配机制，因此 `JetStreamPublish` 的挂载能路由，但没有确认
可等，也没有流的状态可以用来检查期望。违反期望的发布在这里成功，而服务器会拒绝它，因此一条乐观
并发的调用链在真实环境跑起来之前什么也证明不了。

这个 Broker 从内部怎样实现这套契约，读框架文档里的
[实例讲解](https://powersemmi.github.io/ruststream/latest/broker-authors/example-nats/)。
