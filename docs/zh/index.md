# ruststream-nats { #ruststream-nats }

**`ruststream-nats`** 把 [RustStream](https://powersemmi.github.io/ruststream/) 服务订阅到 NATS 的
subject 上，并向这些 subject 发布消息。一个处理器绑定到 Core NATS 的 subject，或者绑定到一个
JetStream 消费者。NATS 会把回复和请求对应起来，因此服务可以发出一次请求并等待答复。JetStream 的
一次发布会说明它对流的期望，期望不成立时流就拒绝这次发布。

启用 `testing` feature 后，测试运行服务的生产应用：`NatsBroker` 在进程内运行，不需要 NATS 服务器，
也可以对着运行中的服务器。

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

服务把自己的处理器挂载在 `NatsBroker` 上：

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:handler"

--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## 这个 crate 提供什么 { #what-the-crate-offers }

crate 参考就是这一切的教科书，每个主题在其中都是一节：

- [三个订阅描述符](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#the-subscription-descriptors)，
  对应 NATS 订阅的三种形态：一个 subject、一个通配符、一个 JetStream 拉取消费者。
- [两个发布策略](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#publishing)：
  普通的 Core NATS，以及等待流确认消息的那个。
- 一次 JetStream 发布对自身声明的
  [去重标识和对流的期望](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#what-one-jetstream-message-states-about-itself)。
- [确认与延迟重新投递](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#acknowledgement-and-delayed-retry)：
  JetStream 消费者在服务器上了结一次投递，Core NATS 的 subject 则根本不了结。
- crate 填写的
  [AsyncAPI 文档](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#the-asyncapi-document)，
  以及[测试](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#testing)：`TestApp`
  测试套件下的生产应用，在进程内运行，或对着运行中的服务器。

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[NATS 参考](https://docs.rs/ruststream-nats)** - crate 本身：描述符、策略、单条消息的设置和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 安装、教程和 Broker 列表。
- :material-language-rust: **[框架参考](https://docs.rs/ruststream)** - 订阅者、路由、编解码器、中间件和 CLI。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

NATS 从这一页开始，而它由什么构成写在 [crate 参考](https://docs.rs/ruststream-nats)里。框架本身
和它自己的 crate 一起记录，入口页面在
[RustStream 站点](https://powersemmi.github.io/ruststream/)上。
