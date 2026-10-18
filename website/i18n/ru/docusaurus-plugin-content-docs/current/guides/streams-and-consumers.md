---
title: Стримы и консьюмеры
sidebar_position: 3
description: "Чтение стрима JetStream через durable- и эфемерные pull-консьюмеры, выбор начальной позиции консьюмера, метаданные доставки и пакеты."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Стримы и консьюмеры

Это руководство читает стрим JetStream через durable- и эфемерные консьюмеры, выбирает, с какого места начинает новый консьюмер, читает метаданные доставки и берёт сообщения пакетами.

Стрим хранит сообщения, опубликованные в его субъекты, каждое под своим порядковым номером.
Консьюмер читает стрим в своём темпе и подтверждает сообщения по одному.

Пример - интернет-магазин.
Он исполняет новые заказы, показывает каждое событие заказа на панели мониторинга и сверяет оплаченные заказы с банком.

## Стрим

Стрим - это состояние сервера.
Сервис находит его при запуске и никогда не создаёт, поэтому создайте его один раз утилитой командной строки NATS:

```bash
nats stream add ORDERS --subjects 'orders.*' --defaults
```

`ORDERS` хранит каждое сообщение, опубликованное в `orders.created`, `orders.paid` и любой другой субъект, к которому подходит `orders.*`.
Обычная публикация в такой субъект сохраняется, кто бы её ни отправил.
Если подписка называет стрим, которого на сервере нет, сервис не запускается.

## Durable-консьюмер

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#fulfil"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#fulfil"
```

</TabItem>
</Tabs>

[`JetStreamSubject`](../components/subscriptions/jetstream-subject.md) принимает субъект, который читает консьюмер, и стрим, который его хранит.
Субъект служит фильтром консьюмера, поэтому `fulfil` читает из `ORDERS` только `orders.created`.
Если фильтр должен отличаться от субъекта, его задаёт [`filter_subject`](../components/subscriptions/filter-subject.md).
Сервис создаёт консьюмер при запуске со всеми настройками дескриптора.

[`durable`](../components/subscriptions/durable.md) даёт консьюмеру имя `fulfilment`.
Сервер хранит его позицию, поэтому перезапущенный сервис продолжает с того места, где остановился.
Реплики сервиса делят один durable-консьюмер, и каждое сообщение достаётся одной из них.
Это аналог [очереди](subjects-and-queue-groups.md) в JetStream.

Обработчик читает место доставки в стриме через [`Ctx<StreamSequence>`](../components/context/index.md).
[Контекст JetStream](../components/context/index.md) содержит ещё имена стрима и консьюмера, порядковый номер в консьюмере, счётчик доставок и число сообщений, которые ещё ждут выдачи.
В ручной форме обработчик принимает `Context<'_, JetStreamContext>` и читает те же ключи через `ctx.context(..)`.
На доставке Core NATS каждый ключ возвращает `None`.

## Эфемерный консьюмер

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#dashboard"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#dashboard"
```

</TabItem>
</Tabs>

Дескриптор без `durable` создаёт эфемерный консьюмер.
У каждого процесса он свой, и сервер удаляет его после закрытия подписки.
Поэтому каждая реплика сервиса показывает каждое событие заказа.

[`DeliverPolicy`](../components/subscriptions/deliver-policy.md) задаёт, с какого места начинает новый консьюмер.
`All`, значение по умолчанию, начинает с первого сообщения в стриме, а `New` - со следующего опубликованного.
`Last`, `LastPerSubject`, `ByStartSequence` и `ByStartTime` начинают с последнего сообщения, с последнего сообщения каждого субъекта, с порядкового номера или с момента времени.
Политика действует один раз, при создании консьюмера. Дальше durable-консьюмер продолжает со своей позиции.

## Пакеты

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#reconcile"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#reconcile"
```

</TabItem>
</Tabs>

Обработчик, который принимает `&[Order]`, получает [пакет](../components/subscriptions/batches.md) сообщений и выполняется один раз на весь пакет.
Возвращённый исход завершает доставку каждого сообщения пакета.

## Приложение

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/streams_and_consumers.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/streams_and_consumers.rs#app"
```

</TabItem>
</Tabs>

Размер пакета задаёт точка монтирования: `batch(100)` просит у сервера до ста сообщений за одну выборку.
[`pull_expires`](api:ruststream_nats::JetStreamSubject::pull_expires) в дескрипторе задаёт, сколько выборка их ждёт, здесь одну секунду. После этого обработчик получает пакет меньшего размера.
По умолчанию выборка ждёт пять секунд.

Ответ `fulfil` публикуется через Core NATS, как у брокера по умолчанию.
Руководство [Публикация в стримы](publishing-to-streams.md) сохраняет ответы в стриме.
Подписки Core и JetStream работают через одно соединение, поэтому один сервис свободно сочетает обе модели.

[`max_ack_pending`](../components/subscriptions/max-ack-pending.md) ограничивает, сколько доставок консьюмер держит без подтверждения. По умолчанию это 1024.
Как завершается каждая доставка, рассказывает руководство [Подтверждение и гарантии доставки](acknowledgement.md).
