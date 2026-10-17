---
title: Подключение к промышленному кластеру
sidebar_position: 9
description: "Подключение сервиса на NATS к промышленному кластеру: несколько серверов, файл .creds, TLS с центром сертификации кластера и тест конфигурации."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Подключение к промышленному кластеру

В промышленном кластере несколько серверов, он требует от сервиса назвать себя и работает через TLS.
Всё это - настройки [`ConnectOptions`](../components/broker/connect-options.md) из async-nats, которые брокер принимает до установки соединения.

## Серверы, учётные данные и TLS

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
async-nats = "0.50"
```

Серверы и пути к двум файлам развёртывание передаёт в переменных окружения.
[`NatsBroker::new`](../components/broker/index.md) принимает один адрес или список через запятую, и клиент выбирает из них тот, к которому подключится.
[`with_options`](api:ruststream_nats::NatsBroker::with_options) добавляет `ConnectOptions` и по-прежнему никуда не подключается.

Файл `.creds` содержит JWT пользователя и seed его ключа NKey. Так сервис называет себя в Synadia Cloud и в учётных записях, настроенных через `nsc`.
Приложение читает файл и передаёт его содержимое в `ConnectOptions::with_credentials`, который сразу его разбирает.
`ConnectOptions::with_credentials_file` читает файл сам, но он асинхронный, а приложение строится в синхронной функции.
Если файл не разбирается, сервис останавливается до подключения.
Для сервера, настроенного с именем пользователя и паролем, есть [`ConnectOptions::with_user_and_password`](../components/broker/security.md), с токеном - `with_token`, с одним только ключом NKey - `with_nkey`.

`add_root_certificates` называет центр сертификации, который подписал сертификаты серверов, а `require_tls` отклоняет соединение без TLS.
`add_client_certificate` добавляет собственный сертификат сервиса, если кластер требует взаимной аутентификации TLS.
Файлы сертификатов клиент читает при подключении.
`name` показывает сервис в мониторинге сервера, а `connection_timeout` ограничивает каждую попытку подключения.

## Сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`app` строит брокер из окружения и передаёт его в `service`, ту же функцию, которую вызывают тесты.
Рантайм подключается при запуске.
Дальше клиент сам переподключается, когда сервер пропадает, и восстанавливает подписки.
[Генерируемый документ AsyncAPI](../components/asyncapi/server.md) указывает хост и порт каждого сервера, но никогда не учётные данные.

Клиент, созданный вне фреймворка, например общий с другим кодом процесса, становится подключённым брокером через [`ConnectedNatsBroker::from_client`](api:ruststream_nats::ConnectedNatsBroker::from_client).
В синхронную функцию приложения подходит только обычный `NatsBroker`, поэтому везде, где настройку можно выразить через `with_options`, используйте его.

## Тест конфигурации

Тесты запускают рабочий `service` с рабочим `broker`, которому переданы тестовые учётные данные, через `TestApp::start`.
[NATS внутри процесса](../components/testing/index.md) разбирает адреса так же, как клиент, и никуда не подключается, поэтому учётные данные и путь к сертификату не доходят ни до какого сервера.
Второй тест передаёт `broker` файл `.creds`, который не разбирается, и ожидает ошибку.
Сами аутентификация и TLS работают только на настоящем кластере.
