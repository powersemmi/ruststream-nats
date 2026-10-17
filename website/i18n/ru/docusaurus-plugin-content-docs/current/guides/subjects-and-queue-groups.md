---
title: Субъекты, шаблоны и очереди
sidebar_position: 2
description: "Субъекты Core NATS, чтение многих субъектов по шаблону, работа, разделённая в очереди, и порядок каждого ключа в параллельных партициях воркеров."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Субъекты, шаблоны и очереди

Это руководство называет субъекты Core NATS, читает многие из них по шаблону, делит работу между репликами в очереди и сохраняет порядок каждого ключа в параллельных партициях воркеров.

Субъект - это имя из токенов, разделённых точками, например `orders.eu.created`.
Core NATS отдаёт сообщение каждой подписке с подходящим субъектом, которая есть в этот момент, и ничего не хранит.

Пример - сервис заказов.
Он упаковывает европейские заказы, выставляет по ним счета и записывает в аудит каждое событие заказа во всех регионах.

## Очередь

`pack` читает `orders.eu.created`.
Сервис работает в нескольких репликах, и каждый заказ нужно упаковать один раз.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#pack"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#pack"
```

</TabItem>
</Tabs>

[`CoreSubject`](../components/subscriptions/core-subject.md) читает один субъект.
[`queue_group`](../components/subscriptions/queue-group.md) включает подписку в очередь `packers`.
Сервер отдаёт каждое сообщение одному участнику очереди, поэтому каждая реплика упаковывает свою долю заказов.
Для сервера одно имя - одна очередь, даже на разных субъектах.
Подписка вне очереди или в другой очереди всё равно получает свою копию.

## Параллельные партиции воркеров по клиентам

`bill` читает тот же субъект без очереди, поэтому получает каждый заказ, который получает `pack`.
Он выставляет счета по нескольким заказам одновременно, не больше чем по восьми, и сохраняет порядок заказов каждого клиента.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#bill"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#bill"
```

</TabItem>
</Tabs>

`workers(8, by_key)` запускает восемь партиций воркеров.
Доставки с одним [ключом](../components/subscriptions/keys.md) выполняются друг за другом в одной партиции воркеров.
В ручной форме та же настройка - [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).
Собственного ключа у NATS нет, поэтому ключ передаётся в заголовке `nats-partition-key` ([`PARTITION_KEY_HEADER`](api:ruststream_nats::PARTITION_KEY_HEADER)), и его задаёт отправитель.
`CustomerKey` - контракт заголовков с одним полем под этим именем.
`Headers<CustomerKey>` читает ключ доставки, а `with_headers(&key)` записывает его в счёт.
Поэтому счета одного клиента сохраняют порядок и для следующего читателя.
Заказ без этого заголовка не проходит проверку контракта, и его доставку завершает политика декодирования, которая по умолчанию отбрасывает сообщение.

Заголовок NATS - это текст: имя состоит из печатных символов ASCII без двоеточия, а значение - одна строка.
Публикация с заголовком, который этим правилам не отвечает, возвращает ошибку.

## Шаблон

`audit` читает каждое событие заказа во всех регионах.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#audit"
```

</TabItem>
</Tabs>

[`CoreWildcard`](../components/subscriptions/core-wildcard.md) читает каждый субъект, к которому подходит шаблон.
`*` заменяет один токен: `orders.*.created` читает создание заказов во всех регионах.
`>` заменяет один или несколько последних токенов: `orders.>` читает каждый субъект внутри `orders`.

## Приложение

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/subjects_and_queue_groups.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/subjects_and_queue_groups.rs#app"
```

</TabItem>
</Tabs>

Шаблон читает многие субъекты, поэтому у отложенной копии его доставки нет единственного субъекта, куда её отправить.
Точка монтирования называет его через [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md), здесь это `orders.retry`, который шаблон тоже читает.
Его называет каждая регистрация на шаблон, даже если её обработчик никогда не просит переобработку, а без него сервис не запускается.
Руководство [Переобработка и недоставленные сообщения](retries.md) рассказывает о копиях подробнее.

## Доставка at-most-once

Core NATS доставляет сообщение тем подпискам, которые существуют в момент его прихода.
Сообщение, опубликованное, когда ни одна подписка ему не подходит, пропадает.
У доставки нет и подтверждения, поэтому обработчик, который не справился, не получит сообщение от сервера повторно.
Если важно каждое сообщение, субъект должен принадлежать стриму: как его читать, показывает руководство [Стримы и консьюмеры](streams-and-consumers.md).

Тесты в файле примера запускают приложение внутри процесса и проверяют копию каждой подписки, шаблон и порядок счетов по каждому клиенту.
