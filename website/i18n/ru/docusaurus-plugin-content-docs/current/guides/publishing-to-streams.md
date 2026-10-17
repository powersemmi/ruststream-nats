---
title: Публикация в стримы
sidebar_position: 5
description: "Публикация ответов и сообщений в стримы JetStream, однократное хранение повтора по идентификатору сообщения и отказ записи по устаревшей версии."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Публикация в стримы

Это руководство публикует ответы и сообщения в стримы JetStream, сохраняет повторное сообщение один раз и отклоняет запись, сделанную по устаревшей версии.

Обычная публикация уходит через Core NATS, и никто её не подтверждает.
[`JetStreamPublish`](../components/publishing/jetstream-publish/index.md) ждёт, пока стрим подтвердит каждое сообщение.
Поэтому сообщение, которое стрим отклонил, - это ошибка, которую видит обработчик, а не тихая потеря.

Пример - сервис заказов.
Он подтверждает каждый заказ, архивирует его и хранит историю каждой учётной записи клиента.
Стримы, в которые он пишет, уже есть на сервере:

```bash
nats stream add ORDERS --subjects 'orders.placed' --defaults
nats stream add CONFIRMATIONS --subjects 'confirmations.orders' --defaults
nats stream add ARCHIVE --subjects 'archive.orders' --defaults
nats stream add ACCOUNTS --subjects 'accounts.*' --defaults
```

## Ответ в стрим

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#confirm"
```

</TabItem>
</Tabs>

`confirm` отвечает на каждый заказ сообщением `Confirmation` в `confirmations.orders`.
Приложение ниже отправляет этот ответ через `JetStreamPublish::default().expect_stream("CONFIRMATIONS")`.
[`expect_stream`](../components/publishing/jetstream-publish/expect-stream.md) называет стрим, который должен хранить субъект, и публикация, которую принял бы другой стрим, отклоняется.
Заказ подтверждается только после того, как стрим сохранил подтверждение.
Если подтверждение отклонено, доставка заказа завершается ошибкой, и заказ возвращается.

Субъект ответа лежит вне субъектов `ORDERS`.
Ответ, сохранённый в стриме, который читает обработчик, снова пришёл бы к этому обработчику.

## Однократное сохранение

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#archive"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#archive"
```

</TabItem>
</Tabs>

Заказ может быть доставлен дважды, и тогда `archive` публикует его копию дважды.
[`message_id`](../components/publishing/jetstream-publish/message-id.md) даёт каждой копии одного заказа один и тот же идентификатор, который передаётся в заголовке `Nats-Msg-Id`.
Стрим сохраняет первую копию и подтверждает повтор, не сохраняя его, поэтому `index` читает каждый заказ один раз.
Стрим помнит идентификаторы в течение своего окна дубликатов: две минуты, если стрим не задаёт другое значение.

`message_id` описывает одно сообщение, поэтому это шаг публикации, а не настройка точки монтирования.
Шаги даёт [`JetStreamPublishSteps`](api:ruststream_nats::JetStreamPublishSteps) из прелюдии крейта.
Тело обработчика, которое вызывает такой шаг, ограничивает свой слот типом `Out<impl Publisher<Options = JetStreamOptions>, _>`.
Это ограничение привязывает обработчик к JetStream. Только здесь обработчик называет брокер.

## Отказ в устаревшей записи

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#accounts"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#accounts"
```

</TabItem>
</Tabs>

История учётной записи 42 - это субъект стрима `accounts.42`.
Клиент меняет адрес электронной почты учётной записи, опираясь на версию, которую прочитал последней: порядковый номер последнего события учётной записи в стриме.
[`expect_last_subject_sequence`](../components/publishing/jetstream-publish/expectations.md) заставляет стрим сохранить событие, только если последнее сообщение субъекта всё ещё стоит под этим номером.
Два клиента, прочитавшие одну версию, не могут записать оба: вторая запись отклоняется, и `change_email` её отбрасывает.
`expect_last_sequence` проверяет весь стрим, а не один субъект, а `expect_last_message_id` - идентификатор последнего сообщения стрима.

## Приложение

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/publishing_to_streams.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/publishing_to_streams.rs#app"
```

</TabItem>
</Tabs>

Каждая публикация JetStream ждёт подтверждения от стрима.
Вне доставки хук запуска принимает политику, и [`publish_ack`](../components/publishing/jetstream-publish/publish-ack.md) возвращает это подтверждение: стрим, порядковый номер и то, видело ли сообщение окно дубликатов.
Публикация Core NATS подходит там, где потеря сообщения допустима, например для панели мониторинга из руководства [Подтверждение и гарантии доставки](acknowledgement.md).

Тесты в файле примера запускают приложение внутри процесса: стрим внутри процесса проверяет идентификаторы сообщений и ожидания так же, как сервер.
