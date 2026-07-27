# 白盒 Kafka 参数分析

## 1. 输入配置

| 配置 | 当前值 | 判断 |
|---|---:|---|
| Kafka 客户端 | confluent-kafka-go v2 / librdkafka | 参数名以 librdkafka 为准 |
| Topic 分区 | 4 | 当前每个 Pod 约负责 1 个分区 |
| Pod 数 | 4 | Consumer Group 成员数与分区数相等 |
| 应用层批次上限 | 10,000 条 | 需要区分应用批次与 Broker Fetch |
| `fetch.min.bytes` | 默认 1 B | 基本不等待数据量，响应更及时 |
| `fetch.max.wait.ms` | 用户按 Kafka 通用名描述为 500 ms | librdkafka 通常对应 `fetch.wait.max.ms`，需核对实际配置 |
| `max.partition.fetch.bytes` | 默认 1 MB | 当前最值得先核对的 Fetch 上限 |
| `fetch.max.bytes` | 默认 50 MB | 4 分区总 Fetch 上限，不解除单分区 1 MB 限制 |
| `max.poll.interval.ms` | 默认 300 s | 处理批次期间允许很长时间不 Poll |
| `session.timeout.ms` | 30 s | 基本合理 |
| `heartbeat.interval.ms` | 默认 3 s | 主要由 librdkafka 后台心跳维护 |
| Offset | 手动，批级提交 | At-Least-Once；提交失败可能重放 |
| 单 Pod Consumer | 1 个 | 正常情况下一个 Pod 处理一个分区 |

## 2. 关键结论

### 2.1 10,000 条应用批次不等于一次 Broker Fetch

在 confluent-kafka-go 中，应用通常通过多次 `Poll()` 或事件读取积累出自己的 10,000 条 `processBatch`。Broker Fetch 受每分区字节上限限制。

当前每个 Consumer 主要负责一个分区，而：

```text
max.partition.fetch.bytes = 1 MB
```

意味着单次从该分区返回的数据通常受约 1 MB 限制。粗略估算：

| 平均消息大小 | 单次1MB Fetch理论消息数 |
|---:|---:|
| 200 B | 约 5,200 条 |
| 500 B | 约 2,100 条 |
| 1 KB | 约 1,000 条 |
| 2 KB | 约 500 条 |

真实值还会受压缩、协议开销、最大消息大小和 Fetch 时机影响。因此，在没有真实消息大小前，不能断言 10,000 条会由一次 Fetch 返回。

### 2.2 10,000 条批次的到达时间

在理想均匀流量下：

```text
当前5,000 QPS：10,000 ÷ 5,000 = 2秒
目标50,000 QPS：每Pod约12,500 QPS
目标50,000 QPS：10,000 ÷ 12,500 = 0.8秒/Pod批次
```

这只是消息积累时间。必须再加上：

```text
反序列化 + 批次内按key汇聚 + 批量数据库写入 + Offset提交
```

只有完整批处理 P99 稳定低于批次积累速度，Consumer Lag 才不会持续增长。

## 3. 当前最重要的风险

### 3.1 `max.partition.fetch.bytes=1MB` 可能限制批次填充速度

如果真实消息平均 1 KB，一个 Fetch 只有约 1,000 条，应用需要多次 Fetch 才能积累 10,000 条。此时增加 `fetch.max.bytes=50MB` 没有直接作用，因为单分区上限仍是 1 MB。

建议先获取：

```text
消息平均、P95、最大序列化字节数
```

然后按下式估算：

```text
目标单Fetch消息数 × P95消息字节数 × 1.5～2
```

选择 `max.partition.fetch.bytes` 候选值，例如 4 MB 或 8 MB，并确认 Broker 侧 `message.max.bytes`、副本 Fetch 上限和网络内存允许。

不要直接把该参数调到非常大，因为它会增加单次 Fetch 内存峰值和 Rebalance 后恢复压力。

### 3.2 Kafka 通用参数名需要映射到 librdkafka

confluent-kafka-go v2 使用 librdkafka 参数，不能直接把 Java 参数名复制到代码。重点核对：

- `fetch.wait.max.ms`：librdkafka 对应的等待时间参数；
- `fetch.min.bytes`；
- `max.partition.fetch.bytes`；
- `fetch.max.bytes`；
- `queued.min.messages`；
- `queued.max.messages.kbytes`；
- `max.poll.interval.ms`；
- `session.timeout.ms`。

应用层的“最多 10,000 条”很可能是 `processBatch` 自己累积的上限，不是 librdkafka 的 `max.poll.records`。需要查看实际 Go 代码确认批次形成逻辑。

### 3.3 Offset 提交失败后继续消费会放大重复累计

当前流程：

```text
processBatch成功
→ Commit重试3次
→ 仍失败则告警
→ 继续消费下一批
```

这符合 At-Least-Once，但如果数据库是 Token 增量累加，Pod 重启、Rebalance 或提交响应丢失后会重复执行同一批次。

因此需要至少满足一个条件：

1. 数据库保存分区 Checkpoint，并与批量 Token Upsert 在同一事务中推进；
2. 每批生成稳定 `batch_id`，数据库唯一约束确保同一批只累计一次；
3. 采用可重放的绝对快照，而不是不可幂等的增量相加。

在完成幂等保护前，Offset Commit 失败后继续消费不是吞吐优化点，而是计费准确性风险。最低限度应限制未确认批次数量，并监控 Commit 失败后的重放和 Token 差异。

## 4. 首轮参数建议

### 4.1 不建议立刻改变应用批次 10,000

先保持 10,000 作为基线，测量：

- 从第一条消息进入批次到数据库提交完成的 P50/P95/P99；
- 每批原始消息数和汇聚后唯一 key 数；
- 每批实际 SQL 数、事务数和数据库往返次数；
- 每批序列化字节数；
- Consumer Lag；
- Heap、GC、Rebalance 和 Commit 失败。

### 4.2 Fetch 参数实验

先不改业务聚合逻辑，做以下隔离实验：

| 方案 | `max.partition.fetch.bytes` | `fetch.min.bytes` | 等待时间 |
|---|---:|---:|---:|
| A | 1 MB（当前基线） | 1 B | 当前值 |
| B | 4 MB | 256 KB | 50～100 ms |
| C | 8 MB | 1 MB | 50～100 ms |

每组观察：

- 达到应用 10,000 条批次的时间；
- Fetch 次数和单次字节数；
- 批次处理 P99；
- Consumer Lag；
- Pod 内存峰值；
- Kafka Broker 网络和请求负载。

如果 B/C 相比 A 没有明显降低批次积累时间，或者内存、Lag、Broker 网络变差，应恢复 A。

### 4.3 `max.poll.interval.ms`

当前 5 分钟不是立即风险，但过于宽松会延长“处理线程卡住后才被发现”的时间。不能未经测量直接降到很小。

建议先测得完整批处理 P99，再设：

```text
max.poll.interval.ms ≥ 完整批处理P99 × 3
```

例如：

| 完整批处理 P99 | 候选 `max.poll.interval.ms` |
|---:|---:|
| 5 秒 | 30 秒 |
| 10 秒 | 60 秒 |
| 20 秒 | 120 秒 |

同时保留足够故障发现速度，不能为了避免 Rebalance 继续无限增大到几分钟。

### 4.4 会话参数

`session.timeout.ms=30s`、心跳约 3s 在当前配置下可以先保持。只有发现网络抖动、Broker 延迟或误判失活时，才单独调整，不要和 Fetch 批次参数同时大幅修改。

## 5. 白盒判断公式

### Fetch容量

```text
单Fetch可容纳消息数 ≈
  max.partition.fetch.bytes ÷ P95消息字节数
```

### Pod输入

```text
Pod输入QPS = 总QPS × 该Pod分配分区数 ÷ 总分区数
```

当前 4 分区、4 Pod、目标 50k QPS 时：

```text
每Pod约12,500 QPS
```

### 批处理安全性

```text
批次处理吞吐 > Pod输入QPS × 1.5
完整批处理P99 < max.poll.interval.ms ÷ 3
```

### 内存

```text
批次内存 ≈ 消息对象内存 + Map/聚合对象 + SQL参数缓存 + GC余量
```

不能只用 Kafka 原始字节数估算 Go Heap，需用 `pprof` 或运行时指标校准。

## 6. 还缺少的决定性输入

要给出“10,000 应改为多少”的最终结论，还需要：

1. 单条消息平均/P95/最大序列化大小；
2. 10,000 条批次的实际处理耗时 P50/P95/P99；
3. 一批 10,000 条汇聚后有多少唯一 key；
4. 汇聚结果实际执行几条 SQL、几个事务；
5. 数据库批量写入耗时和死锁/锁等待；
6. 当前 5k QPS 下 Consumer Lag 和每批形成时间；
7. Go 代码实际如何从 Poll 事件积累出 10,000 条。

## 7. 当前结论

基于现有配置，暂时不建议直接调整应用批次条数。优先顺序是：

1. 核对实际 librdkafka 参数名和应用批次形成代码；
2. 获取消息 P95 字节数，判断 1 MB 分区 Fetch 是否过小；
3. 测量 10,000 条批次完整处理 P99；
4. 验证 Offset Commit 失败重放是否会重复累计；
5. 仅在白盒数据证明有瓶颈时，实验 4 MB/8 MB Fetch 和 5,000/10,000/20,000 应用批次。

在没有消息样例和批处理耗时前，最可信的判断是：

```text
10,000 条不一定需要调整；
max.partition.fetch.bytes=1MB 很值得优先核对；
Offset提交失败后的增量累计幂等是比批次大小更高优先级的问题。
```
