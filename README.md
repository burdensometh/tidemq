# TideMQ

TideMQ 是单节点、持久化、分区式 TCP 消息代理。Java 21 服务在 Linux 上运行，使用 loopback 上的 v1 二进制协议。生产者可以用稳定的身份和序号找回写入回执。消费组按成员代分配分区，保留连续确认水位、乱序完成记录和已确认接收的投递次数。失败消息达到重试上限后进入该组的死信日志。

## 快速运行

需要 JDK 21、Bash 和支持目录 fsync 与同目录原子替换的 Linux 文件系统。构建和运行只使用 JDK 标准库。样例、测试、演示及基准都可离线执行。

```bash
bash scripts/build.sh
java -jar build/tidemq.jar config/broker.properties
```

服务打印 `TIDEMQ_READY` 后，另开终端调用自带客户端。默认数据目录是相对启动目录的 `var/tidemq`，监听 `127.0.0.1:7410`。配置将端口设为 `0` 时，使用就绪行报告的实际端口。

```bash
client=(java -cp build/tidemq.jar tidemq.ClientMain 127.0.0.1 7410)
"${client[@]}" topics
"${client[@]}" produce checkout 0 orders - order-7 created
"${client[@]}" produce checkout 0 orders - order-7 created
"${client[@]}" consume warehouse reader-1 orders 1
"${client[@]}" group warehouse
"${client[@]}" flow warehouse
"${client[@]}" partition orders 0
"${client[@]}" deadletters warehouse
"${client[@]}" retention orders 0
```

首次生产返回分区、偏移和落盘时间。同一生产者、序号、分区及相同键和内容的重发返回原回执，`replayed=true`。同一身份带不同消息会收到 `PRODUCER_CONFLICT`。将分区指定为 `-` 时按 UTF-8 键的字节数组路由。键值可用 Java 客户端直接发送二进制，CLI 也支持 `produce-file` 和从标准输入逐行生产。跨进程重发时调用方保存生产者身份、序号和消息。

Java 客户端提供 `TideClient` 的底层操作，也提供 `ResumableProducer` 和 `GroupConsumer`。生产辅助类可在传输中断时用原始请求找回回执。消费辅助类在后续轮询重连、刷新会话代，并在结算时校验批次属于当前连接。

## 行为与配置

- 顺序属于分区。首次消费从当前最早保留偏移开始。重复投递、乱序确认与并发分区都可以出现。业务处理应按分区及偏移幂等。
- 组的订阅在第一次成功 JOIN 时固定。现存成员名字按字典序排序，订阅分区按话题及分区号排序，再轮转分配。加入、退出和同名重连推进持久成员代，旧代的租约失效。成员可用 `ASSIGN` 更新在当前会话的分配。
- 拉取先预留。标准客户端完整读取并解码 PULL 成功帧，再发送 `ACCEPT`，此时才增加持久尝试次数。发送成功前断线产生的预留可直接释放。已接受的租约在超时、离开代或断线时可重新投递。最后一次已接受租约失效后，代理先持久化死信，再推进组水位。
- ACK 只能结算当前代、当前成员会话中已接受的租约。一批 ACK 在一次组快照内更新。水位是下一个未完成偏移，晚到 ACK 和终态死信只能关闭各自的洞。ACK 回复丢失时可观察组水位，并容忍重复读取。
- 仅回收连续的已封口前缀段。段右端必须不大于所有订阅组的最小连续水位，且达到配置的最短保留年龄。离线组仍参与。无订阅组时使用分区结束偏移。回收永远保留活跃段，并保留独立存储的生产和死信回执。
- `delivery.max.batch` 限制每条批操作的记录数。`delivery.max.inflight` 限制每成员的同时预留及已接受租约总数。`delivery.window` 限制水位之后每分区可探测的偏移范围。帧预算会进一步限制 PULL 或死信查询返回行数。
- `segment.bytes` 改变未来滚段阈值。话题及分区拓扑落盘后固定。重试阈值取进程使用的配置，重启时会重判已丢失活跃租约且达到阈值的条目。
- 物理存储发生写入或持久边界异常时，代理报告 `STORAGE_FAILURE`，并要求重启以恢复索引。v1 能修复活跃段或日志最后一个物理长度不足的记录，完整记录校验失败会拒绝打开。

配置位于 [config/broker.properties](config/broker.properties)。当前版本将业务转移和磁盘提交串行化，TCP 连接使用有上限的虚拟线程。协议面向同一主机内的受信客户端。生产回执索引与死信历史随独立日志增长。持久离线组会阻挡保留期限到期的段，规划数据卷时要计入这一行为。

## 原生验证

```bash
bash scripts/build.sh
bash scripts/test.sh
bash scripts/demo.sh
bash scripts/bench.sh 1024
bash scripts/expiry-proof.sh
```

`expiry-proof.sh` 是到期清扫的可复跑负载与不变量证明：白盒负载把账本扩到 20 万租约，核对每周期实际检查的条目数恒等于到期项（固定 8 个到期时，总租约从 1 千涨到 20 万，检查数始终为 8、耗时基本持平；到期数从 1 涨到 2048 时检查数同步增长），并通过真实 TCP 验证预留到期只释放槽位、已接受到期按原始尝试次数重投或终止，以及 ACK/换代后的旧到期记录不会驱逐新租约或产生第二份死信。

测试运行真实 TCP、段和组快照，使用可控时钟与故障回调覆盖落盘前后、回执前断线、组代替换和死信持久化。演示启动独立 Java 服务，创建两个生产者、两个组及三个成员，输出真实序号、分配、乱序确认、水位、死信、保留结果和重连回执。基准报告包含批拉取并完成、慢组的水位洞与窗口空拉取、保留扫描与物理删除，输出延迟分位数并核对结果。

离线段读取和样本重建说明见 [samples/README.md](samples/README.md)。二进制布局、错误码及恢复顺序见 [docs/v1.md](docs/v1.md)。

## 源码入口

| 职责 | 入口 |
| --- | --- |
| 生产、路由和收据 | `ingress/Ingress`、`ingress/ReceiptIndex` |
| 段、偏移索引与回收 | `segment/SegmentStore`、`segment/PartitionLog`、`segment/RecordFile` |
| 成员代、分配和水位 | `group/GroupDirectory`、`group/GroupStore`、`group/AckFrontier` |
| 流量窗口、接受和确认 | `delivery/DeliveryEngine`、`delivery/LeaseBook` |
| 死信与幂等恢复 | `deadletter/DeadLetters`、`delivery/DeliveryEngine.recover` |
| TCP 服务和客户端 | `wire/TcpBroker`、`wire/Session`、`wire/TideClient` |

`Broker` 绑定业务交接和持久化边界。运行入口为 `ServerMain`，命令行客户端为 `ClientMain`，离线段检查为 `SegmentMain`。
