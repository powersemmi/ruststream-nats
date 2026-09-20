# 基准测试 { #benchmarks }

在 NATS 客户端和你的处理器之间，框架在每条消息上都要花时间：订阅的流、解码、分发、ack。这一页
说明它花了多少，参照物是同样的活用 `async-nats` 手写一遍。

同一个进程把一个场景跑三遍。**裸**直接驱动 `async-nats`。**适配器**手写驱动这个 crate 自己的
类型，上面没有处理器，也没有运行时。**服务**是用户会写的那个应用。其余一切都保持相同：连接选项、
订阅、消费者配置、确认的位置、解码成同一个类型、载荷字节、tokio 运行时和构建。这套流程属于框架
本身，写在
[RustStream 基准测试页](https://powersemmi.github.io/ruststream/latest/zh/benchmarks/#methodology)
上；这一页公布它在这台机器上得出的结果。

由此得到两个差值。适配器对裸客户端，是这个 crate 的消费者和发布者在它们包装的客户端之上的开销：
这个仓库负责的就是这个数字。服务对裸客户端，是一个完整服务的开销，适配器和运行时加在一起。运行时
自己的开销就是两列之间的距离，它按 Broker 分别公布，因为运行时占比在不同 Broker 之间有差别，这
本身就是关于传输和运行时如何相接的事实。

## 数字 { #the-numbers }

三个交错轮次中的最佳值，括号里是最差的一轮。越大越好。

<div id="benchmark-results" data-benchmark-results="../../benchmarks/results.json" data-benchmark-labels='{"loading": "正在加载公布的结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "ruststream-nats", "framework": "RustStream 服务", "adapterOverhead": "适配器对裸客户端", "overhead": "服务对裸客户端", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "machine": "机器", "os": "操作系统", "broker": "Broker", "build": "构建", "versions": "版本", "measured": "测量于", "unavailable": "读不到结果。它们公布在 {url}。", "unknownSchema": "公布的结果声明的 schema 是 {schema}，这一页不渲染它。"}'></div>

表格由浏览器从上一次运行写下的文档读出，所以这一页上没有任何会过期的副本。

Core NATS 是这些数字能落在的最薄的底座：那里的一次投递就是一次 subject 匹配加一个消息体，服务器
不做任何确认，所以每一列相对前一列加了多少，周围没有传输开销遮住。

标为「无法区分」的一行，是两半之间的差值小于各自多次运行之间离散范围的那一行。凡是传输本身比
分发贵得多的地方，这就是诚实的结果：JetStream 的每次投递都要把一次确认送回服务器，这样大小的
差值就淹没在一次确认里面。低于运行噪声的数字读起来像是从未测到过的精度，所以不公布。

标为「受 Broker 限制」的一行，是传输让消费者等待服务器等得够频繁，以至于一条消息的开销里有一半
花在等待上。这样的数字讲的更多是服务器和回环地址，而不是这个 crate。Core NATS 不向消费者收取
每次投递的往返：服务器自己把匹配推进订阅里。JetStream 拉取消费者收的是每批一次的拉取请求，以及
一次不等回答就发出的确认。拿来对照的那个往返就是下面机器一栏里的探测值，所以这笔账可以重算。

同一次运行的机器可读形式在
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-nats/latest/benchmarks/results.json)，
框架的站点用它拼出跨 Broker 的汇总表。

## 机器 { #the-machine }

<div id="benchmark-environment"></div>

构建标志和数字一起公布，因为它们会改变这些数字。用 `-C target-cpu=native` 构建出的二进制给出的
结果，换一台机器就复现不了，所以这条 recipe 在构建前先把这个变量清空。

## 这些数字不代表什么 { #what-they-do-not-mean }

这里只有一个消费者、一个 subject、一个很小的消息体，以及一台跑在回环地址上的服务器。它测的是一次
投递在这个 crate 里的开销，不是 NATS 能扛多少。这里的一行也不能拿去和另一个 Broker 公布的一行
比较：不同的传输在每条消息上做的事并不一样。

一次运行的测量窗口从第一次投递开始，到最后一个处理器返回为止，两半都是这样。框架在处理器结束之后
才确认投递，而这个时刻处理器自己看不到，所以一次运行携带的数百万次确认里的这一次，在两边都落在
数字之外。

JetStream 的数字取自一个内存存储、work-queue 保留策略的流。这样服务器底下的磁盘就不会进入一次
关于分发的测量。放在文件存储上的流回答的是另一个问题，而且回答的是服务器，不是这个 crate。

这些数字是一台机器在某一天的快照。它们按需重测，从不放进 CI：共享 runner 的噪声比这一页要讲的
差值还大。

## 自己跑一遍 { #running-it-yourself }

```bash
just bench
```

这条 recipe 从 `docker-compose.test.yml` 起停测试台，跑完两个场景，然后把测到的结果写回
`docs/benchmarks/results.json`。它要花十分钟左右，并且需要整台机器。消息条数不是固定的：一次试探
运行会把它定下来，使得每一次被测量的运行在所在机器上都不短于五秒。
