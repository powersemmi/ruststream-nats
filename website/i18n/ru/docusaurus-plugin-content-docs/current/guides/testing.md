---
title: Тестирование сервиса на NATS
sidebar_position: 8
description: "Тестирование сервиса на NATS внутри процесса с TestApp: рабочее приложение, стримы, ответы, задержки и тот же тест на сервере NATS."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на NATS

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [NATS внутри процесса](../components/testing/index.md) или на сервере.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в `card.payments`, его сохраняет стрим `CARD_PAYMENTS`, а читает durable-консьюмер.
Обработчик отвечает квитанцией в `card.receipts`.
Платёж без карты отбрасывается.
Платёж сверх мгновенного лимита банка откладывается: обработчик просит доставить его снова через минуту с [`retry_after`](../components/settlement/retry-after.md).
После трёх доставок платёж уходит в `card.review`, [субъект для недоставленных сообщений](../components/settlement/dead-letters.md).
Приложение строит `service(broker)`, поэтому тест выбирает адрес брокера сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-nats` в `[dev-dependencies]`.

## Платёж и квитанция

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`NatsBroker`](../components/broker/index.md) к NATS внутри процесса.
Соединение по адресу не устанавливается, и стрим не нужно создавать заранее.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<NatsBroker>()`.
`message(..).publish()` публикует платёж в субъект, который объявляет его тип, и стрим сохраняет его.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик завершил доставку и что дошло до `card.receipts`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на сервере ниже.

## Платёж без карты

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#declined"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#declined"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Задержка без ожидания

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, и [`advance`](api:ruststream::testing::TestApp::advance) переводит их вперёд.
Минута `HOLD` проходит мгновенно.
Консьюмер придерживает платёж так же, как сервер, и доставляет его снова по истечении задержки.
Третья доставка - последняя, которую разрешает точка монтирования, поэтому платёж уходит в `card.review` в том виде, в каком пришёл.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует NATS внутри процесса

NATS внутри процесса читает то же, что прочитал бы клиент: адреса и те параметры соединения, которые меняют то, что доставляет сервер (`no_echo`, префикс инбокса).
Маршрутизирует и хранит он так же, как сервер:

- Субъекты сопоставляются с шаблонами `*` и `>`, а очередь отдаёт каждое сообщение одному участнику.
- Стрим ведёт порядковые номера и окно дубликатов и проверяет ожидания, которые задаёт публикация.
- Консьюмер начинает с того места, которое задаёт его политика доставки, и заполняет метаданные [контекста JetStream](../components/context/index.md).
- Подтверждение, отрицательное подтверждение с задержкой и без неё и прекращение доставки действуют так же, как на сервере. Доставка без исхода возвращается после `ack_wait`.
- Запрос доходит до отвечающей стороны приложения, а на запрос, который никто не читает, сразу приходит «no responders».

Он отклоняет то же, что отклоняют клиент или сервер: субъект с пробельными символами, заголовок, у которого нет формы в NATS, сообщение больше 1 МиБ и публикацию JetStream, которую не принимает ни один стрим.
Стрим внутри процесса хранит то, что читают консьюмеры приложения, а подписка находит каждый стрим, который называет.
Собственный список субъектов стрима и его пределы, `max_ack_pending`, аутентификация и TLS требуют сервера.

## Тот же тест на сервере

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к работающему серверу NATS.
Тест берёт адрес из `NATS_TEST_URL` через `live::url`, вспомогательную функцию самого примера, и пропускается, если переменная не задана.
Стрим - это состояние сервера, поэтому тест сначала создаёт `CARD_PAYMENTS` через собственное соединение, а в конце удаляет его.
Вместе со стримом удаляется и его консьюмер, поэтому следующий запуск начинается с чистого состояния.
Проверки те же, что и внутри процесса, без изменений.

```bash
cargo test
NATS_TEST_URL=nats://localhost:4222 cargo test -- --test-threads=1
```
