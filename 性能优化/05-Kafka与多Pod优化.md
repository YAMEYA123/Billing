# Kafka 与多 Pod 优化

## 1. 不直接扩到 24 分区

分区多不会自动导致 Rebalance 更频繁；Pod 频繁启停、心跳超时、处理线程阻塞和扩缩容抖动才是主要诱因。但分区越多，单次重新分配和状态恢复的工作量越大。

因此采用渐进策略：

```text
先优化并压测4分区
→ 不足时验证8分区
→ 再按需验证12/16分区
→ 仍不足才考虑24分区
```

## 2. 分区数计算

```text
所需分区数 = 峰值QPS ÷ 单分区安全QPS × 安全系数
```

单分区安全吞吐必须在完成批量聚合、数据库刷新和 Redis 优化后重新测试。验收不只看正常 50k QPS，还要验证一个 Pod 故障后的吞吐和追平能力。

## 3. 分区与 Pod 配比

分区数不需要等于 Pod 数：

```text
8分区 + 4个Pod
12分区 + 6个Pod
16分区 + 8个Pod
```

让一个 Pod 持有少量多个分区，可以减少 Consumer Group 成员数量和扩缩容敏感度。

## 4. Rebalance 优化

### Cooperative Sticky

优先采用 Cooperative Sticky 分配策略，使大多数 Pod 继续消费，只转移必要分区。具体配置依赖 Sarama、franz-go 或 confluent-kafka-go 的实际版本。

### Poll 与处理解耦

Kafka Poll Loop 不执行数据库、Redis、元数据或话单网络调用。数据进入分区有界队列，由 Worker 处理。队列接近容量时 Pause 对应分区，恢复后 Resume。

### 优雅停机

1. Readiness 失败；
2. 停止拉取新消息；
3. 完成短事务；
4. 保存可安全提交的状态；
5. 主动离开 Consumer Group；
6. 进程退出。

仍需通过 DB Checkpoint 支持 `kill -9` 和节点故障。

### Kubernetes

- 配置 PodDisruptionBudget；
- 使用保守的 `maxUnavailable`；
- 发布时观察 Lag，超过阈值暂停；
- 扩容根据持续 Lag 和追平时间，而非瞬时 CPU；
- 缩容设置更长观察窗口和冷却期；
- 静态成员只在实例 ID 稳定且唯一时使用。

## 5. Topic 扩容方式

不直接修改旧 Topic 分区数。建议创建新版本 Topic：

1. 网关双写；
2. 新 Consumer Group 影子消费；
3. 比较每窗口消息数、请求数和 Token 总量；
4. 故障注入验证 Rebalance；
5. 切换正式来源；
6. 旧 Topic 保留一个回滚周期。

## 6. 收益与风险

| 修改 | 收益 | 风险 |
|---|---|---|
| 渐进扩分区 | 按真实容量扩展 | 新 Topic 双写复杂 |
| Cooperative Sticky | 减少全量停顿 | 客户端版本兼容 |
| Poll/处理解耦 | 避免心跳超时 | 队列和背压复杂 |
| PDB与滚动发布 | 降低成员抖动 | 发布速度变慢 |
| DB Checkpoint恢复 | Rebalance不重计 | 事务与恢复实现复杂 |
