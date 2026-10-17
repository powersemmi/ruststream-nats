---
title: 连接生产环境
sidebar_position: 9
description: "把 NATS 服务连接到生产集群：多台服务器、.creds 文件、使用集群证书颁发机构的 TLS，以及对配置的测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 连接生产环境

生产集群有多台服务器，会要求服务表明身份，并且使用 TLS 通信。
这些都是 async-nats [`ConnectOptions`](../components/broker/connect-options.md) 的设置，Broker 在发起任何连接之前接收它。

## 服务器、凭据与 TLS {/* #servers-credentials-and-tls */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
async-nats = "0.50"
```

部署环境通过环境变量提供服务器地址和两个文件路径。
[`NatsBroker::new`](../components/broker/index.md) 接收一个地址或以逗号分隔的地址列表，客户端从中选一个去连接。
[`with_options`](api:ruststream_nats::NatsBroker::with_options) 附加 `ConnectOptions`，此时仍不发起任何连接。

`.creds` 文件保存用户 JWT 及其 NKey 种子，Synadia Cloud 和用 `nsc` 配置的账户就是这样识别服务的。
应用读取这个文件，把内容交给 `ConnectOptions::with_credentials`，后者立即解析它。
`ConnectOptions::with_credentials_file` 会自己读取文件，但它是 `async` 的，而应用是在同步函数中构建的。
无法解析的文件会让服务在连接之前就停止。
服务器配置了用户名和密码时，使用 [`ConnectOptions::with_user_and_password`](../components/broker/security.md)；配置了令牌时使用 `with_token`；只配置了 NKey 时使用 `with_nkey`。

`add_root_certificates` 指定为服务器证书签名的证书颁发机构，`require_tls` 拒绝不使用 TLS 的连接。
集群要求双向 TLS 时，`add_client_certificate` 加入服务自己的证书。
客户端在连接时读取证书文件。
`name` 让服务显示在服务器的监控中，`connection_timeout` 限制每次连接尝试的时间。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`app` 根据环境构建 Broker，并把它交给 `service`，测试调用的也是这个函数。
运行时在启动时建立连接。
此后，服务器下线时客户端会自行重连，并恢复各个订阅。
[生成的 AsyncAPI 文档](../components/asyncapi/server.md)报告每台服务器的主机和端口，从不包含凭据。

在框架之外构建的客户端，比如与进程中其他代码共用的客户端，可以用 [`ConnectedNatsBroker::from_client`](api:ruststream_nats::ConnectedNatsBroker::from_client) 变成已连接的 Broker。
只有普通的 `NatsBroker` 适用于同步的应用函数，所以只要 `with_options` 能表达某项设置，就用它。

## 测试配置 {/* #testing-the-configuration */}

测试在 `TestApp::start` 下启动生产环境的 `service`，配上用测试凭据构建的生产环境 `broker`。
[进程内 NATS](../components/testing/index.md) 像客户端一样解析地址，但不发起任何连接，所以凭据和证书路径永远不会到达服务器。
第二个测试把一个无法解析的 `.creds` 文件交给 `broker`，并预期得到错误。
认证和 TLS 本身只在真实集群上进行。
