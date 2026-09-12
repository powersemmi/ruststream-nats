# ruststream-nats { #ruststream-nats }

**`ruststream-nats`** 把 [RustStream](https://powersemmi.github.io/ruststream/) 服务订阅到 NATS 的
subject 上，并向这些 subject 发布消息。一个处理器绑定到 Core NATS 的 subject，或者绑定到一个
JetStream 消费者。NATS 会把回复和请求对应起来，因此服务可以发出一次请求并等待答复。JetStream 的
一次发布会说明它对流的期望，期望不成立时流就拒绝这次发布。

`testing` feature 在进程内运行服务的处理器，不需要 NATS 服务器。

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

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[NATS 指南](nats.md)** - Core 订阅、JetStream、请求-响应和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-nats)** - 这个 crate 在 docs.rs 上的 rustdoc。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点只讲 NATS 特有的内容。其余的一切都在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
