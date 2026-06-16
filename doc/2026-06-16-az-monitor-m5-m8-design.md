# M5-M8 细颗粒度监控自研设计方案

- 作者：lifc
- 日期：2026-06-16
- 分支：feature/3az-moniter-exporter
- 背景：基于《kafka_监控方案调研报告》，M5/M6/M7/M8 是 danielqsj/kafka-exporter 的硬缺口，在此分支上自研扩展实现。

---

## 一、整体方案

在现有 `kafka_exporter.go` 基础上新增一个文件 `az_metrics.go`，将所有自研逻辑封装在其中，对主文件只做最小侵入。

```
kafka_exporter/
├── kafka_exporter.go     ← 只改 4 处（加字段、加 flag、调用新方法、修 M8 gate）
└── az_metrics.go         ← 新文件，自研逻辑全部在此
```

**设计原则：**
- `kafka_exporter.go` 的改动全部是"加法"，不删除任何现有逻辑
- `az_metrics.go` 完全独立，上游 upstream 更新时冲突最少
- 若未传入 `--az.broker-map`，新指标不注册、不上报，完全向后兼容

---

## 二、新增指标

所有新指标使用 `yig_kafka_` 前缀，与原生 `kafka_` 指标明确区分。

### M5/M6 — AZ 副本合规

| 指标 | Labels | 类型 | 含义 |
|------|--------|------|------|
| `yig_kafka_partition_az_spread_ok` | topic, partition | 运行时 | 1=副本覆盖 ac/yj/lf 三个 AZ；0=不合规 |
| `yig_kafka_partition_replication_factor` | topic, partition | 运行时 | 副本总数（来自 sarama `Replicas()` 长度） |
| `yig_kafka_partition_isr_count` | topic, partition | 运行时 | **当前实际** ISR 副本数（来自 sarama `InSyncReplicas()` 长度） |
| `yig_kafka_topic_min_insync_replicas` | topic | **配置值** | topic 的 min.insync.replicas **Kafka 配置项**，非运行时 ISR 数，用于检查写入门槛是否被调低 |

> `isr_count` 与 `min_insync_replicas` 是两件不同的事：
> - `isr_count < replication_factor` → 有副本掉出同步（运行时故障）
> - `min_insync_replicas != 2` → topic 写入门槛配置被改低（配置合规问题）

**覆盖范围：** 所有 topic（含 `__consumer_offsets`，即 M6），与现有 `--topic.filter` / `--topic.exclude` 保持一致。当前生产环境未设置过滤，覆盖全部 ~70 个 topic。

### M7 — 分 AZ Broker 在线统计

| 指标 | Labels | 含义 |
|------|--------|------|
| `yig_kafka_broker_online_by_az` | az | 各 AZ 在线 broker 数（ac/yj/lf） |

> broker 在线总数直接使用现有 `kafka_brokers` 指标，不重复上报。

### M8 — Partition 无 Leader

| 指标 | Labels | 含义 |
|------|--------|------|
| `yig_kafka_partition_offline` | topic, partition | 1=offline（leader 不可用）；0=正常 |

> **修复原有 bug**：原代码在 `client.Leader()` 失败时只打日志，整条时序从 Prometheus 消失，PromQL 无法可靠探测。改为明确上报 0/1。

---

## 三、AZ Broker 映射

通过 CLI flag `--az.broker-map` 传入，格式：

```
--az.broker-map="ac=27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29|yj=11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10|lf=39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2"
```

- AZ 组之间用 `|` 分隔
- 每组格式：`azName=brokerID1,brokerID2,...`
- 共 3 个 AZ，各 16 个 broker，总计 48 个

当前生产环境映射：

| AZ | Broker IDs |
|----|-----------|
| ac | 27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29 |
| yj | 11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10 |
| lf | 39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2 |

---

## 四、数据流

```
collect() 每次 scrape 触发
│
├── 1. collectAZBrokerMetrics(ch)            [M7]
│      client.Brokers() → 遍历在线 broker
│      按 brokerAZ map 分组计数
│      → yig_kafka_broker_online_by_az{az=ac/yj/lf}
│
├── 2. collectTopicMinISR()                  [M5/M6 配置检查]
│      sarama.ClusterAdmin.DescribeConfig()
│      → map[topic]int64 (min.insync.replicas)
│      （ClusterAdmin 用完即关，不持久化）
│
└── 3. getTopicMetrics(topic) goroutine      [M5/M6/M8，每个 topic 并发]
       for each partition:
       │
       ├── client.Leader()                   ← M8 gate，前置检查
       │    → error: emit yig_kafka_partition_offline=1
       │              continue（跳过该 partition 剩余采集，避免无效 Kafka 请求）
       │    → ok:    emit yig_kafka_partition_offline=0
       │              继续执行下面所有采集
       │
       ├── client.GetOffset() × 2            [现有逻辑不变]
       ├── client.Replicas()
       │    → len(replicas) → yig_kafka_partition_replication_factor
       │    → 遍历副本 broker ID，查 brokerAZ map → AZ 集合去重
       │      → len(azSet)==3 ? 1:0 → yig_kafka_partition_az_spread_ok
       ├── client.InSyncReplicas()
       │    → len(ISR) → yig_kafka_partition_isr_count
       ├── minISR map[topic] → yig_kafka_topic_min_insync_replicas
       └── preferred replica / under-replicated [现有逻辑不变]
```

---

## 五、kafka_exporter.go 改动点（4 处）

### 5.1 Exporter struct 加字段

```go
type Exporter struct {
    // ... 现有字段不变 ...
    brokerAZ map[int32]string // broker ID → AZ name，空 map 时新指标不上报
}
```

### 5.2 kafkaOpts 加字段 + main() 加 flag

```go
// kafkaOpts 加：
azBrokerMap string

// main() 加：
toFlagStringVar("az.broker-map", "AZ broker map, format: az1=id1,id2|az2=id3,id4", "", &opts.azBrokerMap)
```

### 5.3 collect() 调用新方法

```go
// 在现有 broker 统计之后插入：
if len(e.brokerAZ) > 0 {
    e.collectAZBrokerMetrics(ch)
    minISR = e.collectTopicMinISR()
}
```

### 5.4 getTopicMetrics() 修复 M8 并调用 AZ 指标

```go
// leader 检查改为前置 gate：
broker, err := e.client.Leader(topic, partition)
if err != nil {
    klog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
    if len(e.brokerAZ) > 0 {
        ch <- prometheus.MustNewConstMetric(yigPartitionOffline, prometheus.GaugeValue, 1, topic, partStr)
    }
    continue  // 跳过该 partition 后续所有采集
}
if len(e.brokerAZ) > 0 {
    ch <- prometheus.MustNewConstMetric(yigPartitionOffline, prometheus.GaugeValue, 0, topic, partStr)
}
// 现有 kafka_topic_partition_leader 上报逻辑不变
```

---

## 六、az_metrics.go 结构

```go
package main

// 指标描述符（包级变量）
var (
    yigBrokerOnlineByAZ         *prometheus.Desc
    yigPartitionOffline         *prometheus.Desc
    yigPartitionAZSpreadOK      *prometheus.Desc
    yigPartitionRF              *prometheus.Desc
    yigPartitionISRCount        *prometheus.Desc
    yigTopicMinISR              *prometheus.Desc
)

func init() { /* 注册所有描述符 */ }

// parseAZBrokerMap 解析 --az.broker-map 字符串 → map[int32]string
func parseAZBrokerMap(s string) map[int32]string

// collectAZBrokerMetrics M7：按 AZ 统计在线 broker 数
func (e *Exporter) collectAZBrokerMetrics(ch chan<- prometheus.Metric)

// collectTopicMinISR M5/M6：用 ClusterAdmin 拉取所有 topic 的 min.insync.replicas 配置值
func (e *Exporter) collectTopicMinISR() map[string]int64

// emitPartitionAZMetrics M5/M6：在已有 replicas/ISR 数据基础上计算并上报 AZ 分布指标
func (e *Exporter) emitPartitionAZMetrics(ch chan<- prometheus.Metric, topic, partStr string,
    replicas []int32, inSyncReplicas []int32, minISR map[string]int64)
```

---

## 七、Docker 部署（无影响）

现有 Dockerfile 使用 `ENTRYPOINT [ "/bin/kafka_exporter" ]`，新 flag 通过 docker-compose `command:` 传入：

```yaml
services:
  kafka-exporter:
    image: kafka-exporter:feature-3az-moniter-exporter
    command:
      - "--kafka.server=172.108.16.17:9092"
      - "--az.broker-map=ac=27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29|yj=11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10|lf=39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2"
      # 其他现有参数不变
    ports:
      - "9308:9308"
```

**若未传 `--az.broker-map`，所有 `yig_kafka_*` 指标不注册，完全向后兼容。**

---

## 八、告警规则（供 Prometheus 配置参考）

```yaml
groups:
  - name: yig_kafka_m5_m8
    rules:
      # M5/M6 — 副本不合规
      - alert: YigKafkaReplicaNotCompliant
        expr: yig_kafka_partition_replication_factor != 3 or yig_kafka_partition_az_spread_ok == 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "副本不合规: {{ $labels.topic }}/p{{ $labels.partition }}"

      # M5/M6 — ISR 缩容（运行时实际同步副本数不足）
      - alert: YigKafkaIsrShrunk
        expr: yig_kafka_partition_isr_count < yig_kafka_partition_replication_factor
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "ISR缩容: {{ $labels.topic }}/p{{ $labels.partition }} ISR={{ $value }}"

      # M5/M6 — min.insync.replicas 配置不合规（写入门槛配置被改低）
      - alert: YigKafkaMinIsrConfigWrong
        expr: yig_kafka_topic_min_insync_replicas != 2
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "min.insync.replicas配置异常: {{ $labels.topic }} 当前值={{ $value }}，应为2"

      # M7 — 单 AZ broker 数量告警
      - alert: YigKafkaAzBrokerLow
        expr: yig_kafka_broker_online_by_az < 12
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "AZ {{ $labels.az }} broker 仅剩 {{ $value }}/16"

      # M8 — Partition 无 Leader（最高优先级）
      - alert: YigKafkaPartitionOffline
        expr: yig_kafka_partition_offline == 1
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Partition offline: {{ $labels.topic }}/p{{ $labels.partition }}"
```
