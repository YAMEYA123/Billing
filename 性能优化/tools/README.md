# Kafka 导出数据白盒分析脚本

`analyze_kafka_export.py` 是一个只读离线脚本：不连接 Kafka、数据库或生产环境，只读取你导出的消息文件，统计参数分析所需的数据。

## 支持输入

- `pipe`：默认，按分隔符解析；支持普通文本和 `.gz`；
- `jsonl`：每行一个 JSON；
- 使用 `-` 从标准输入读取。

## 你的样例格式

你现在提供了字段名，可以在导出文件保留首行 Header 的情况下使用 `--header` 和字段名传参。脚本不会自行判断哪些字段属于业务 key 或 Token。

```text
timestamp  timestamp（单位需要确认）
x-span-id  链路字段，通常不作为计费聚合 key
request_id  请求/事件标识候选
domain_id  计费主题（你已确认）
project_id  项目维度，是否属于聚合唯一键需要确认
api-key  API Key 标识
region  站点/地域候选
service_id / custom_resident_model_id / resident_model_id  模型或服务标识候选
factor1 / factor2 / factor3  不同计费因子（你已确认），可一次传入多个
```

即使有 Header，仍必须通过参数明确指定时间戳、`--key-fields` 和 `--factor-fields`，避免把链路 ID 或不属于业务唯一键的字段纳入统计。脚本会拒绝缺少这些映射的命令。

## 运行示例

## 已确认的无 Header 字段映射

根据当前字段顺序（从 0 开始）：

```text
0  timestamp（毫秒）
1  x-span-id
2  request_id（事件唯一 ID）
3  domain_id（计费主题）
4  project_id
5  api-key
6  region
7  service_id
8  custom_resident_model_id
9  resident_model_id
10及以后  factor1、factor2、factor3……（需要按文件实际列数展开）
```

正式聚合 key 使用：

```text
timestamp计算出的5分钟window_start + domain_id + resident_model_id + api-key
```

因为导出没有 Header，脚本无法判断第 10 列以后是否全部都是 factor。上传文件后先统计实际列数，再显式传入，例如：

```bash
--factor-fields 10,11,12,13
```

如果第 10 列以后还混有非计费字段，需要从参数中排除，不能把所有尾部字段都算成因子。

### 管道输入

对于当前已确认的无 Header 格式，推荐直接使用预设：

```bash
python3 性能优化/tools/analyze_kafka_export.py export.txt \
  --preset loomi-pipe \
  --json-output analysis.json
```

该预设等价于 `timestamp=0`、`request_id=2`、聚合字段 `3,9,5`、五分钟窗口，
并把第 10 列到行尾自动识别为 `factor1`、`factor2`……。只有在确认第 10 列之后
全部都是计费因子时才能使用；如果尾部还有其他业务字段，请改用显式的
`--factor-fields 10,11,12`。

```bash
python3 性能优化/tools/analyze_kafka_export.py export.txt \
  --timestamp 0 \
  --timestamp-unit ms \
  --key-fields 3,9,5 \
  --event-id 2 \
  --factor-fields 10,11,12 \
  --batch-size 10000 \
  --max-partition-fetch-bytes 1048576 \
  --json-output analysis.json
```

使用字段名的形式：

```bash
python3 性能优化/tools/analyze_kafka_export.py export-with-header.txt \
  --header \
  --timestamp timestamp \
  --timestamp-unit ms \
  --key-fields domain_id,resident_model_id,api-key \
  --event-id request_id \
  --factor-fields factor1,factor2,factor3 \
  --json-output analysis.json
```

正式聚合唯一键已经确认是：

```text
window_start（timestamp按固定5分钟计算）
+ domain_id
+ resident_model_id
+ api-key
```

因此正式命令应使用 `--key-fields domain_id,resident_model_id,api-key`，脚本会另外按五分钟窗口统计唯一键数量。`factor1/factor2/factor3` 仍作为不同用量因子单独统计，不放入聚合 key。

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
- 各计费因子的总量和分布（兼容 input/output Token 模式）；
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
