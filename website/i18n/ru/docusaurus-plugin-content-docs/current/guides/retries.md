---
title: Переобработка и недоставленные сообщения
sidebar_position: 6
description: "Переобработка доставки NATS сразу или позже, предел попыток и стрим для недоставленных сообщений, на стриме, на субъекте и на шаблоне."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Переобработка и недоставленные сообщения

Это руководство переобрабатывает доставку сразу или позже, ограничивает число попыток и сохраняет то, что раз за разом не обрабатывается, в стриме для [недоставленных сообщений](../components/settlement/dead-letters.md): на консьюмере стрима, на субъекте Core и на шаблоне.

В NATS нет механизма недоставленных сообщений.
JetStream умеет ограничивать число доставок сообщения, но последнюю доставку никуда не отправляет.
Поэтому предел и субъект для недоставленных сообщений объявляет точка монтирования, а фреймворк применяет их к обеим моделям.

Пример - платёжный сервис, который проводит платежи и отправляет возвраты через банк.
Недоставленные сообщения он хранит в отдельном стриме:

```bash
nats stream add PAYMENTS --subjects 'payments.settle' --defaults
nats stream add DEAD --subjects 'dead.*' --defaults
```

## Переобработка на стриме

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#settle"
```

</TabItem>
</Tabs>

`outcome` превращает отказ банка в исход обработчика.
Если банк занят, [`retry_after`](../components/settlement/retry-after.md) повторяет обращение через 30 секунд.
На консьюмере JetStream задержка передаётся в отрицательном подтверждении: сервер придерживает платёж и повторно доставляет его тому же консьюмеру с тем же порядковым номером в стриме.
Ничего не публикуется, и процесс, остановленный в промежутке, ничего не теряет.
Если банк молчит, `retry` повторяет обращение сразу, а платёж по отклонённой карте отбрасывается.

## Предел попыток и недоставленные сообщения

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) считает все доставки одного платежа, включая первую.
На консьюмере JetStream счёт ведёт сам сервер, поэтому истёкший [`ack_wait`](../components/settlement/ack-wait.md) тоже учитывается.
Пятая доставка не переобрабатывается: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) публикует её в `dead.payments` с полезной нагрузкой и заголовками в том виде, в каком они пришли.
Отброшенный платёж завершается и никуда не уходит, поэтому среди недоставленных сообщений лежат только платежи, у которых кончились попытки.
Консьюмер с пределом, но без субъекта для недоставленных сообщений прекращает свою последнюю доставку.

`dead.payments` - обычная публикация, а Core NATS не хранит то, что никто не читает.
Её сохраняет стрим `DEAD`, поэтому недоставленное сообщение ждёт, пока его кто-нибудь посмотрит.
Ни один субъект, который читает приложение, не подходит к субъекту для недоставленных сообщений. Иначе исчерпанная доставка вернулась бы к обработчику, который от неё отказался.

## Чтение недоставленных сообщений

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` читает недоставленные платежи из стрима `DEAD`, как любой другой стрим, и пишет каждый в журнал, чтобы его посмотрел человек.

## Переобработка на субъектах Core

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refunds"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refunds"
```

</TabItem>
</Tabs>

Core NATS не умеет придерживать сообщение.
На подписке Core переобработка - это копия доставки, которую публикует фреймворк: `retry` публикует её сразу, `retry_after` - по истечении задержки.
Число доставок копия хранит в заголовке [`x-ruststream-retry-count`](api:ruststream::runtime::RETRY_COUNT_HEADER), и предел читает его оттуда.
Отложенная копия доставляется at-most-once: процесс, который остановился до конца задержки, её теряет.

[`CoreSubject`](../components/subscriptions/core-subject.md) публикует свои копии в собственный субъект.
`send_refund` читает шаблон `refunds.*`, у которого нет единственного субъекта для публикации.
Поэтому приложение называет его через [`out_retry(Publish).to("refunds.retry")`](../components/settlement/out-retry.md). Это субъект, к которому подходит шаблон, и копии возвращаются в `send_refund`.
Регистрация на шаблон без него не запускается, просит её обработчик переобработку или нет.
Третья доставка возврата уходит в `dead.refunds` с числом доставок в заголовке.

Тесты в файле примера запускают приложение внутри процесса с остановленными часами, поэтому `tb.advance(BACKOFF)` проматывает 30 секунд мгновенно.
Работу с обвязкой описывает руководство [Тестирование сервиса на NATS](testing.md).
