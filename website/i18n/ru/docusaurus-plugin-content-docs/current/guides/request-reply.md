---
title: Запрос и ответ
sidebar_position: 7
description: "Запрос к другому сервису через NATS с ожиданием ответа, ответы на запросы в их инбокс, тайм-ауты и «no responders», масштабирование отвечающей стороны."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Запрос и ответ

Это руководство отправляет запрос другому сервису через NATS и ждёт ответа, отвечает на запросы в их инбокс, обрабатывает запрос, на который никто не отвечает, и масштабирует отвечающую сторону.

NATS сам сопоставляет ответ с запросом.
Запрос содержит инбокс, уникальный для него субъект, и ответ публикуется в этот инбокс.

Пример - два сервиса в одном приложении.
Сервис цен отвечает на вопросы о ценах, а сервис заказов спрашивает у него цену каждого заказа.

## Отвечающая сторона

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

Запрос приходит с инбоксом в заголовке `reply-to`, и `ctx.headers().reply_to()` читает его.
`quote` публикует цену через свой слот `Answers` с [`to(inbox)`](../components/request-reply/responders.md), потому что адрес известен только для каждого запроса отдельно.
Поэтому `Price` не объявляет собственного субъекта.
На товар без цены ответа нет, и ожидание запрашивающей стороны заканчивается по тайм-ауту.

Отвечающая сторона читает `prices.quote` в очереди `pricing`.
Каждая реплика сервиса цен входит в очередь, и каждый запрос достаётся одной из них.
На тот же запрос ответит любая отвечающая сторона NATS, например `nats reply prices.quote '{"sku":"book","cents":1250}'` из командной строки.

## Запрашивающая сторона

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#requester"
```

</TabItem>
</Tabs>

`price_order` читает новые заказы из стрима и спрашивает цену каждого в `prices.quote`.
Слот `Pricing` ограничен типом `Out<impl RequestReply, _>`: тело обработчика заявляет, что ему нужен [запрос и ответ](../components/request-reply/index.md), и не называет брокер.
Приложение связывает слот с обычной политикой `Publish`, издатель которой в NATS поддерживает запрос и ответ.
[`request`](api:ruststream::RequestReply::request) публикует вопрос с новым инбоксом и возвращает первое сообщение, пришедшее в этот инбокс.
Вопрос - сырое сообщение, поэтому тело обработчика само кодирует его и декодирует ответ, здесь через `serde_json`.
Ответ на заказ, `Priced`, уходит, как только цена известна.

Запрос может вернуть ошибку двумя способами.
Если субъект не читает ни одна подписка, сервер сразу отвечает «no responders».
Если отвечающая сторона есть, но молчит, запрос возвращает ошибку [`NatsError::RequestTimeout`](api:ruststream_nats::NatsError::RequestTimeout) по истечении тайм-аута, здесь двух секунд.
В обоих случаях заказ ждёт в стриме 30 секунд и спрашивает снова.

## Приложение

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#app"
```

</TabItem>
</Tabs>

Оба слота принимают обычную политику `Publish`: один публикует ответы, другой задаёт вопросы.
Хук запуска спрашивает так же: через `b.after_startup(Publish, ..)` и `request` на издателе, который он получает.

Тесты в файле примера запускают обе стороны в одном приложении внутри процесса.
NATS внутри процесса направляет запрос отвечающей стороне самого приложения, отвечает «no responders», если субъект не читает ни одна подписка, и завершает по тайм-ауту запрос к молчащей отвечающей стороне.
