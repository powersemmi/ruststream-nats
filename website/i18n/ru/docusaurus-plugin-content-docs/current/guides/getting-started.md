---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-nats, локальный сервер NATS, первый подписчик на нём и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-nats`, запускает локальный сервер NATS и первый сервис на нём.

## Установка

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

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-nats` - брокер NATS.
Его фича `testing` добавляет [NATS внутри процесса](../components/testing/index.md), на котором работают тесты.

У крейта брокера две фичи, обе выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `testing` | NATS внутри процесса, на котором работает `TestApp` |
| `asyncapi` | [привязки NATS](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |

Крейт построен на клиенте [async-nats](https://docs.rs/async-nats). Он написан на Rust и шифрует соединение через rustls, поэтому сборке не нужны системные библиотеки.

## Локальный NATS

Для разработки достаточно одного сервера NATS с JetStream:

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

Сервер слушает `localhost:4222`.
Флаг `-js` включает JetStream. С его стримами работают руководства, начиная со [Стримов и консьюмеров](streams-and-consumers.md).
Примеры публикуют сообщения [утилитой командной строки NATS](https://github.com/nats-io/natscli) `nats`.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из субъекта Core NATS `orders.created`.
Заказ с суммой он принимает, а заказ без суммы отбрасывает.
[`NatsBroker::new`](../components/broker/index.md) принимает адрес сервера и только запоминает его.
Соединение открывается при запуске сервиса.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте заказ из другого терминала:

```bash
nats pub orders.created '{"id":1,"amount_cents":1250}'
```

Сервис пишет в журнал `order taken` с номером заказа.
В Core NATS нет подтверждения, поэтому для каждого заказа в журнале есть и `ack / nack failed`: рантайм сообщает, что завершить доставку не удалось.
Руководство [Подтверждение и гарантии доставки](acknowledgement.md) объясняет, когда это важно и как стрим завершает каждое сообщение.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает сервис.

`cargo generate` создаёт такой же сервис с роутером:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-nats templates/nats --name my-service
```

Шаблон `templates/nats-js` создаёт сервис, который читает стрим через durable-консьюмер.

## Первый тест

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `NatsBroker` подключается к NATS внутри процесса, поэтому тестам не нужен сервер:

```bash
cargo test
```

Руководство [Тестирование сервиса на NATS](testing.md) разбирает такие тесты и то, что моделирует NATS внутри процесса.

## Переход с async-nats

Сервис на async-nats сам пишет цикл для каждой подписки и публикует сообщения вручную.
Вот куда переходит каждая часть в RustStream:

| async-nats | RustStream |
|---|---|
| `async_nats::connect` или `ConnectOptions::connect` | [`NatsBroker::new`](../components/broker/index.md) и [`with_options`](../components/broker/connect-options.md) для `ConnectOptions` |
| `client.subscribe` и цикл `while let Some(message)` | обработчик `#[subscriber("subject")]` |
| `client.queue_subscribe` | [`CoreSubject::queue_group`](../components/subscriptions/queue-group.md) ([Субъекты, шаблоны и очереди](subjects-and-queue-groups.md)) |
| `serde_json::from_slice(&message.payload)` | тип параметра обработчика, который декодирует кодек |
| `client.publish` | ответ или параметр `Out`, отправленный через политику [`Publish`](../components/publishing/index.md) |
| `client.request` | [`request`](../components/request-reply/index.md) на слоте `Out<impl RequestReply, _>` ([Запрос и ответ](request-reply.md)) |
| `create_consumer` на стриме и цикл `messages()` | [`JetStreamSubject`](../components/subscriptions/jetstream-subject.md) ([Стримы и консьюмеры](streams-and-consumers.md)) |
| `message.ack()`, `AckKind::Nak`, `AckKind::Term` | исход обработчика: `ack`, `retry`, `retry_after`, `drop` ([Подтверждение и гарантии доставки](acknowledgement.md)) |
| `jetstream.publish` и его подтверждение | [`JetStreamPublish`](../components/publishing/jetstream-publish/index.md) ([Публикация в стримы](publishing-to-streams.md)) |
