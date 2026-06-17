# M5-M8 监控开发进度

- 最后更新：2026-06-16
- 分支：`feature/3az-moniter-exporter`
- 操作机器：meda0004（172.108.16.17）

---

## 已完成

### 代码

| 文件 | 内容 |
|------|------|
| `az_metrics.go` | 新建。6 个 `yig_kafka_*` 指标描述符 + `initAZMetrics()`、`parseAZBrokerMap()`、`collectAZBrokerMetrics()`（含 AZ=0 保底）、`collectTopicMinISR()`（并发拉取）、`emitPartitionAZMetrics()` |
| `az_metrics_test.go` | 新建。7 个单元测试，全部通过，无需 Kafka 连接 |
| `kafka_exporter.go` | 最小改动：新增 `brokerAZ`/`numAZs` 字段、`azBrokerMap` 选项、`--az.broker-map` CLI flag、`initAZMetrics()` 按需调用（有 az.broker-map 才初始化）、`Describe()` 按需注册、`NewExporter()` 解析、`collect()` 接入 M7+minISR、`getTopicMetrics()` 接入 M8 gate + M5/M6 采集 |
| `deploy/docker-compose.yml` | 新建。生产部署配置，含 9 个 kafka.server 和完整 az.broker-map |

### 文档

| 文件 | 内容 |
|------|------|
| `doc/2026-06-16-az-monitor-m5-m8-design.md` | 设计方案（指标规格、数据流、告警规则参考） |
| `doc/2026-06-16-az-monitor-m5-m8-plan.md` | 实现计划（4 个 Task，已全部执行完） |

### Docker 镜像

本地已构建验证：

```bash
docker build --build-arg BIN_DIR=.build/linux-amd64/ -t kafka-exporter:feature-3az-moniter-exporter .
```

---

## 待完成

### 1. 把镜像部署到 meda0004

当前 meda0004 跑的是 `danielqsj/kafka-exporter:latest`，需要替换为自研镜像。

**方式 A — 本地 build 后 save/load（无镜像仓库时）：**

```bash
# 构建
CGO_ENABLED=0 GOOS=linux go build -o .build/linux-amd64/kafka_exporter .
docker build --build-arg BIN_DIR=.build/linux-amd64/ -t kafka-exporter:3az .

# 导出并传到 meda0004
docker save kafka-exporter:3az | gzip > kafka-exporter-3az.tar.gz
scp kafka-exporter-3az.tar.gz root@172.108.16.17:/root/

# 在 meda0004 上加载并切换
ssh root@172.108.16.17
docker load < kafka-exporter-3az.tar.gz
cd /root   # 或 docker-compose.yml 所在目录
docker-compose down && docker-compose up -d
```

**方式 B — 推送到内部镜像仓库（如有）：**

```bash
docker tag kafka-exporter:3az <your-registry>/kafka-exporter:3az
docker push <your-registry>/kafka-exporter:3az
# 然后修改 deploy/docker-compose.yml 中的 image 字段
```

### 2. Prometheus scrape 配置

在 Prometheus 的 `prometheus.yml` 里确认（或新增）以下 job，指向 meda0004:9308：

```yaml
scrape_configs:
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['172.108.16.17:9308']
```

> 如果已有此 job，无需修改，新指标会自动被采集。

### 3. Prometheus 告警规则

完整规则（M1–M8）见 [`doc/alert_rules/yig_kafka_alerts.yml`](alert_rules/yig_kafka_alerts.yml)，直接复制到 Prometheus 规则目录加载即可。

以下为内容速览：

**M5/M6/M7/M8 新规则（自研 exporter 指标）：**

```yaml
groups:
  - name: yig_kafka_m5_m8
    rules:
      # M5/M6 — 副本不合规（RF != 3 或 AZ 分布不均）
      - alert: YigKafkaReplicaNotCompliant
        expr: yig_kafka_partition_replication_factor != 3 or yig_kafka_partition_az_spread_ok == 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "副本不合规: {{ $labels.topic }}/p{{ $labels.partition }}"

      # M5/M6 — ISR 缩容（当前实际同步副本数不足）
      - alert: YigKafkaIsrShrunk
        expr: yig_kafka_partition_isr_count < yig_kafka_partition_replication_factor
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "ISR缩容: {{ $labels.topic }}/p{{ $labels.partition }} ISR={{ $value }}"

      # M5/M6 — min.insync.replicas 配置不合规（写入门槛被调低）
      - alert: YigKafkaMinIsrConfigWrong
        expr: yig_kafka_topic_min_insync_replicas != 2
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "min.insync.replicas配置异常: {{ $labels.topic }} 当前值={{ $value }}，应为2"

      # M7 — 单 AZ broker 数量不足
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

**M1/M2/M3/M4 补充规则（danielqsj 原生指标，调研报告第三章）：**

```yaml
  - name: yig_kafka_m1_m4
    rules:
      # M1 — 单 partition LAG 过高
      - alert: YigKafkaPartitionLagHigh
        expr: kafka_consumergroup_lag > 10000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.consumergroup }}/{{ $labels.topic }}/p{{ $labels.partition }} LAG={{ $value }}"

      # M2 — topic 总 LAG 过高
      - alert: YigKafkaTopicLagHigh
        expr: kafka_consumergroup_lag_sum > 100000
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.consumergroup }}/{{ $labels.topic }} 总LAG={{ $value }}"

      # M3 — 消费者 offset 卡死
      - alert: YigKafkaConsumerStuck
        expr: |
          increase(kafka_consumergroup_current_offset[2m]) == 0
          and on(topic, partition)
          increase(kafka_topic_partition_current_offset[2m]) > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "消费者卡死: {{ $labels.consumergroup }}/{{ $labels.topic }}/p{{ $labels.partition }}"

      # M4 — 高流量 topic 生产者停写
      - alert: YigKafkaProducerStuckHighTraffic
        expr: |
          increase(kafka_topic_partition_current_offset{
            topic=~"accessLogTopic|loggingTopic|yig_meta_async_delete"
          }[5m]) == 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "生产者停写: {{ $labels.topic }}/p{{ $labels.partition }}"

      # M4 — gc topic 中途挂
      - alert: YigKafkaGcTopicStuck
        expr: |
          increase(kafka_topic_partition_current_offset{topic=~"yig_gc_.*"}[10m]) > 0
          and
          increase(kafka_topic_partition_current_offset{topic=~"yig_gc_.*"}[5m]) == 0
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "GC中途中断: {{ $labels.topic }}/p{{ $labels.partition }}"

      # M4 — gc topic 今日未运行
      - alert: YigKafkaGcNotRunToday
        expr: increase(kafka_topic_partition_current_offset{topic=~"yig_gc_.*"}[24h]) == 0
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "GC今日未运行: {{ $labels.topic }}"
```

### 4. M9 消费速率（搁置）

等压测数据确定各 topic 的期望消费速率 baseline 后再实现。数据层已有（`kafka_consumergroup_current_offset`），PromQL 用 `deriv()` 而非 `rate()`。

---

## 关键配置速查

**AZ Broker 映射：**

| AZ | Broker IDs |
|----|-----------|
| ac | 27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29 |
| yj | 11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10 |
| lf | 39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2 |

**生产 exporter 地址：** `meda0004 (172.108.16.17):9308`

**当前运行镜像：** `danielqsj/kafka-exporter:latest`（待替换）
