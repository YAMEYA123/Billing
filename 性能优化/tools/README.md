# Kafka 导出数据白盒分析脚本

`analyze_kafka_export.py` 是一个只读离线脚本：不连接 Kafka、数据库或生产环境，只读取你导出的消息文件，统计参数分析所需的数据。

## 支持输入

- `pipe`：默认，按分隔符解析；支持普通文本和 `.gz`；
- `jsonl`：每行一个 JSON；
- 使用 `-` 从标准输入读取。

## 你的样例格式

按目前提供的样例，可以暂时按以下位置传参，但这些不是脚本默认值，正式分析前必须确认字段含义：

```text
0  timestamp(ms)
1～9  组成聚合key的字段（需要结合真实字段含义确认）
10 input_tokens
11 output_tokens
```

由于示例中没有字段名，必须通过参数明确指定时间戳、`--key-fields`、输入 Token 和输出 Token 字段，避免把不属于业务唯一键的 ID 纳入统计。脚本现在会拒绝缺少这些映射的命令。

## 运行示例

### 管道输入

```bash
python3 性能优化/tools/analyze_kafka_export.py export.txt \
  --timestamp 0 \
  --timestamp-unit ms \
  --key-fields 1,5,6,7,8 \
  --event-id 2 \
  --input-tokens 10 \
  --output-tokens 11 \
  --batch-size 10000 \
  --max-partition-fetch-bytes 1048576 \
  --json-output analysis.json
```

### Gzip 文件

```bash
python3 性能优化/tools/analyze_kafka_export.py export.txt.gz \
  --key-fields 1,5,6,7,8 \
  --event-id 2 \
  --json-output analysis.json
```

### JSON Lines

```bash
python3 性能优化/tools/analyze_kafka_export.py export.jsonl \
  --format jsonl \
  --timestamp event.occurred_at \
  --timestamp-unit ms \
  --key-fields user_id,model_id,api_key_id \
  --event-id event_id \
  --input-tokens input_tokens \
  --output-tokens output_tokens
```

## 输出内容

- 总消息数、无效记录数和文件字节数；
- 消息字节数 P50/P95/P99/最大值；
- 事件时间范围和观测 QPS；
- 输入/输出 Token 总量和分布；
- 全文件唯一 key 和固定五分钟窗口唯一 key；
- 可选的重复 event ID 数量；
- 按 10,000 条模拟应用批次的消息数、字节数、唯一 key 分布；
- 按 `max.partition.fetch.bytes` 估算每次 Fetch 可容纳的消息数；
- 完整 JSON 报告。

## 安全注意事项

- 只在离线文件上运行；
- 不提供 Kafka Broker、用户名、密码或生产 Topic；
- 先脱敏用户、API Key、Prompt 和模型输出；
- 保留字段长度和 Token 数即可；
- JSON 报告可能包含字段值拼接后的 key，正式分享前检查并脱敏。
