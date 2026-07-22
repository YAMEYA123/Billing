# 话单 Outbox 与元数据优化

## 1. 为什么需要 Outbox

定时任务不应扫描不断增长的聚合事实表寻找待上报记录。窗口固化时应在同一数据库事务内生成 Outbox，使待办规模与历史表规模解耦。

## 2. Outbox 参考结构

```text
bill_id, aggregate_id, window_start, virtual_bucket,
status, next_retry_at, retry_count,
lease_owner, lease_expired_at,
payload或payload_reference,
sent_at, last_error, created_at
```

`bill_id` 必须有唯一约束。

状态机：

```text
PENDING → LEASED → SENT
             └→ RETRY → DEAD
```

## 3. 多 Pod 领取

使用短事务 `FOR UPDATE SKIP LOCKED` 或租约字段领取任务。事务提交后再进行元数据查询和 Kafka 发送。

租约过期后其他 Pod 可以接管。租约时间应大于正常批次处理 P99，并保留续租或保守超时机制。

## 4. Kafka 发送幂等

- Producer 开启幂等和 `acks=all`；
- Kafka 消息 Key 使用 `bill_id`；
- 下游结算系统按 `bill_id` 建立业务幂等；
- 保留发送结果、Payload 哈希和 Kafka 元数据用于审计。

Producer 幂等不能解决“Kafka 已成功、数据库尚未标记 SENT 时进程崩溃”，因此下游业务幂等是必要条件。

## 5. 上报吞吐

10 万张话单在 5 分钟内完成，平均约 333 条/秒；若计划在 3 分钟内完成，则约 556 条/秒。建议压测至少 1,700 条/秒，预留约 3 倍余量。

候选压测参数：

- 每批 500、1,000、2,000 条；
- 4、8、16 个构建 Worker；
- Kafka 异步批量发送；
- 数据库批量更新状态。

## 6. 元数据优化

原始事件聚合阶段只使用稳定 ID，不查询远程元数据。话单阶段：

1. 汇总一批 Outbox 中的唯一用户、模型和 API Key ID；
2. 分类型批量查询；
3. 使用 Go 进程内缓存；
4. 在窗口内首次见到 ID 时异步预热；
5. 对不存在的 ID 使用短期负缓存；
6. 限制缓存容量和单批查询大小。

影响金额和归属的字段必须版本化，例如：

```text
pricing_version
account_owner_version
model_billing_version
```

否则重放历史窗口时，使用最新元数据可能生成不同话单。

## 7. 迟到和修正

建议窗口结束后等待 2～3 分钟再正常固化。超过宽限期的数据：

- 下游支持时生成 `revision + 1` 的增补/冲正话单；
- 下游不支持时进入异常对账队列并告警；
- 禁止静默修改已经结算的历史话单。

## 8. 收益、风险和回滚

收益：可靠重试、不漏历史失败任务、可水平扩展、元数据查询量从原始 QPS 降到唯一 ID 数。

风险：重复发送、Outbox 膨胀、缓存过期和租约误回收。

回滚：新 Worker 先发送影子 Topic；保留旧定时任务开关；已发送记录初期只归档不删除。
