---
title: Subject
sidebar_position: 0
description: "Core NATS 在消息到达的那一刻，把它投递给 subject 匹配的每个订阅，以及队列组中的一个成员，并且什么也不存储。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "向 orders.eu.created 的一次发布：服务器把它复制给每个匹配的订阅，以及队列组中的一个成员",
  "text": "以文字阅读本图",
  "publisher": "发布者",
  "server": ["NATS 服务器", "把 subject", "与它此刻", "持有的", "每个订阅", "逐一匹配"],
  "exact": "相同的 subject",
  "one": "* 匹配一个 token",
  "rest": "> 匹配其余部分",
  "other": "不匹配",
  "queue": "队列组 billing",
  "member1": "成员 1 收到",
  "member2": "成员 2 收不到",
  "dropped": ["没有订阅匹配的消息被丢弃：", "Core NATS 什么也不存储"],
  "publishes": "发布"
};

# Subject

Core NATS 在消息到达的那一刻，把它投递给 subject 匹配的每个订阅，以及队列组中的一个成员，并且什么也不存储。

<Diagram name="subjects" words={words} />

subject 是由点分隔的一串 token。订阅的 subject 可以包含通配符。`*` 恰好代表一个 token。`>` 代表一个或多个 token，并且只能放在最后。发布总是给出完整的 subject，不带通配符。

服务器把消息复制给每个与之匹配的订阅。之后才加入的订阅永远看不到这条消息，没有任何订阅匹配的消息就此消失。这就是至多一次投递：服务器既不保存什么，也不结算什么。

队列组分担工作。服务器把每条消息交给组内的一个成员，组外的订阅仍然收到属于自己的一份。组只由名字标识，所以用同一个名字订阅不同 subject 的成员也属于同一个组。

请求是一次给出了应答 subject 的发布，这个 subject 是请求方事先订阅的 inbox。响应方把应答发布到那里。没有任何订阅匹配的请求会立即得到 "no responders" 应答，因此请求方不必等到超时。

从 NATS 2.2 起，消息可以携带消息头。消息头的名字是不含冒号的可打印 ASCII 字符，值只有一行。超过载荷上限的消息会被服务器拒绝，默认上限是 1 MiB。

这一模型就是 NATS 服务器自己的模型（[subject](https://docs.nats.io/nats-concepts/subjects)、[发布-订阅](https://docs.nats.io/nats-concepts/core-nats/pubsub)、[队列组](https://docs.nats.io/nats-concepts/core-nats/queue)和[请求-应答](https://docs.nats.io/nats-concepts/core-nats/reqreply)）。流如何保存消息，见[流](streams.md)；消费者如何读取它们，见[消费者](consumers.md)。
