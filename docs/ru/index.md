# ruststream-nats {#ruststream-nats}

**`ruststream-nats`** подписывает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
субъекты NATS и публикует в них сообщения. Обработчик привязывается к субъекту Core NATS или к
консьюмеру JetStream. NATS сам сопоставляет ответы с запросами, поэтому сервис может отправить
запрос и дождаться ответа. Публикация в JetStream заявляет, чего она ждёт от стрима, и стрим
отвергает её, когда ожидание не выполнено.

Фича `testing` выполняет обработчики сервиса внутри процесса, без сервера NATS.

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-nats = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-nats = { version = "0.7", features = ["testing"] }
```

Сервис монтирует обработчики на `NatsBroker`:

```rust
--8<-- "crates/ruststream-nats/examples/nats_core.rs:handler"

--8<-- "crates/ruststream-nats/examples/nats_core.rs:app"
```

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Руководство по NATS](nats.md)** - подписки Core, JetStream, запрос-ответ и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-nats)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает то, что специфично для NATS. Всё остальное - в
[документации RustStream](https://powersemmi.github.io/ruststream/).
