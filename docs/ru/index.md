# ruststream-nats {#ruststream-nats}

**`ruststream-nats`** подписывает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
субъекты NATS и публикует в них сообщения. Обработчик привязывается к субъекту Core NATS или к
консьюмеру JetStream. NATS сам сопоставляет ответы с запросами, поэтому сервис может отправить
запрос и дождаться ответа. Публикация в JetStream заявляет, чего она ждёт от стрима, и стрим
отвергает её, когда ожидание не выполнено.

С фичей `testing` тест запускает рабочее приложение сервиса: `NatsBroker` работает внутри процесса,
без сервера NATS, или подключается к живому серверу.

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

## Что умеет крейт {#what-the-crate-offers}

Справочник крейта - учебник по всему этому, и каждая тема в нём отдельный раздел:

- [Три дескриптора подписки](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#the-subscription-descriptors),
  по одному на каждую форму подписки NATS: один субъект, шаблон и pull-консьюмер JetStream.
- [Две политики публикации](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#publishing):
  обычный Core NATS и та, что дожидается подтверждения сообщения стримом.
- [Идентификатор дедупликации и ожидания от стрима](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#what-one-jetstream-message-states-about-itself),
  которые одна публикация в JetStream заявляет о себе.
- [Подтверждение и отложенная повторная доставка](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#acknowledgement-and-delayed-retry):
  консьюмер JetStream закрывает доставку на сервере, а субъект Core NATS не закрывает её вовсе.
- [Документ AsyncAPI](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#the-asyncapi-document),
  который заполняет крейт, и [тестирование](https://docs.rs/ruststream-nats/latest/ruststream_nats/index.html#testing):
  рабочее приложение под обвязкой `TestApp`, внутри процесса или против живого сервера.

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Справочник NATS](https://docs.rs/ruststream-nats)** - сам крейт: дескрипторы, политики, настройки одного сообщения, тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - установка, учебник, список брокеров.
- :material-language-rust: **[Справочник фреймворка](https://docs.rs/ruststream)** - подписчики, маршрутизация, кодеки, middleware, CLI.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

С этой страницы NATS начинается, а из чего он состоит - в
[справочнике крейта](https://docs.rs/ruststream-nats). Сам фреймворк описан вместе с его
крейтом, а входные страницы лежат на
[сайте RustStream](https://powersemmi.github.io/ruststream/).
