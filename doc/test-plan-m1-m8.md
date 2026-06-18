# M1-M8 测试方案（呼和测试环境）

- 作者：lifc
- 日期：2026-06-18
- 目标环境：呼和测试集群（172.25.x.x），Prometheus 172.25.42.247:9090
- 操作节点：建议在 172.25.42.247（监控机）上执行所有 docker/curl 命令

---

## 一、前置准备（只做一次）

### 1.1 Broker ID → AZ 映射（已确认）

测试环境 rack=null，按网段分配：

| AZ | IP 段 | Broker IDs |
|----|-------|-----------|
| az1 | 172.25.42.241/242/243 | 1, 2, 3 |
| az2 | 172.25.43.241/242/243 | 4, 5, 6 |
| az3 | 172.25.44.241/242/243 | 7, 8, 9 |

对应关系：1=.42.241, 2=.42.242, 3=.42.243, 4=.43.241, 5=.43.242, 6=.43.243, 7=.44.241, 8=.44.242, 9=.44.243

### 1.2 传送镜像到监控机

在开发机执行：

```bash
docker save kafka-exporter:feature-3az-moniter-exporter | gzip \
  > kafka-exporter-3az.tar.gz

scp kafka-exporter-3az.tar.gz root@172.25.42.247:/root/
```

在 172.25.42.247 上执行：

```bash
docker load < /root/kafka-exporter-3az.tar.gz
docker images | grep kafka-exporter   # 确认镜像存在
```

### 1.3 创建测试环境 docker-compose

在 172.25.42.247 上创建 `/root/kafka-exporter-test/docker-compose.yml`：

```yaml
version: '3'
services:
  kafka-exporter:
    image: kafka-exporter:feature-3az-moniter-exporter
    container_name: kafka-exporter-test
    restart: always
    ports:
      - "9308:9308"
    command:
      - "--kafka.server=172.25.42.241:9092"
      - "--kafka.server=172.25.42.242:9092"
      - "--kafka.server=172.25.42.243:9092"
      - "--kafka.server=172.25.43.241:9092"
      - "--kafka.server=172.25.43.242:9092"
      - "--kafka.server=172.25.43.243:9092"
      - "--kafka.server=172.25.44.241:9092"
      - "--kafka.server=172.25.44.242:9092"
      - "--kafka.server=172.25.44.243:9092"
      - "--az.broker-map=az1=1,2,3|az2=4,5,6|az3=7,8,9"
      - "--kafka.version=2.0.0"
```

启动：

```bash
cd /root/kafka-exporter-test
docker-compose up -d
docker logs kafka-exporter-test   # 确认无报错，看是否有 "AZ info auto-detected" 或 "No broker rack info"
```

### 1.4 配置 Prometheus scrape

在 172.25.42.247 上编辑 `/data2/yig-monitoring/conf/prometheus.yml`，在 `scrape_configs:` 下追加：

```yaml
  - job_name: 'kafka-exporter-test'
    static_configs:
      - targets: ['172.25.42.247:9308']
```

热重载（无需重启 Prometheus）：

```bash
curl -X POST http://172.25.42.247:9090/-/reload
```

### 1.5 加载告警规则

```bash
cp /root/yig_kafka_alerts.yml /data2/yig-monitoring/conf/rules/
# 确认 prometheus.yml 里有 rule_files 加载该目录，否则追加路径
curl -X POST http://172.25.42.247:9090/-/reload
```

在 Prometheus UI（http://172.25.42.247:9090/rules）确认 M1-M8 规则已加载。

---

## 二、冒烟验证（确认 exporter 整体正常）

```bash
# 1. 确认 exporter 在吐指标
curl -s http://172.25.42.247:9308/metrics | head -20

# 2. 确认原有指标存在（M1-M4 基础）
curl -s http://172.25.42.247:9308/metrics | grep "^kafka_brokers "

# 3. 确认新指标存在（M5-M8）
curl -s http://172.25.42.247:9308/metrics | grep "yig_kafka_broker_online_by_az"

# 4. 预期输出（3 个 AZ 各 3 个 broker）
# yig_kafka_broker_online_by_az{az="az1"} 3
# yig_kafka_broker_online_by_az{az="az2"} 3
# yig_kafka_broker_online_by_az{az="az3"} 3
```

---

## 三、各 M 测试步骤

### ⚠️ 已知测试环境现存异常（启动后预计立即触发）

| 现象 | 预期告警 | 说明 |
|------|----------|------|
| BillingTopic RF=1 | YigKafkaReplicaNotCompliant | RF!=3 |
| statTopic ISR 仅 5,3（broker 7 不在 ISR） | YigKafkaIsrShrunk | 运行时 ISR 缩容 |

这两条属于**真实存在的问题**，不是 exporter bug，告警触发是正确行为。

---

### M7 — 分 AZ Broker 在线统计

**目标指标：** `yig_kafka_broker_online_by_az`

**验证步骤（无损）：**

```bash
# 确认三个 AZ 各自 broker 数量正确
curl -s http://172.25.42.247:9308/metrics | grep yig_kafka_broker_online_by_az
```

预期：每个 AZ 显示 3（测试环境每 AZ 3 台 broker）。

**破坏性测试（验证告警触发）：**

```bash
# 在 AZ1 的一台机器上停 kafka（选一台，不要停全部）
ssh root@172.25.42.241 "systemctl stop kafka"

# 等 30s（scrape 周期）后，检查指标
curl -s http://172.25.42.247:9308/metrics | grep yig_kafka_broker_online_by_az
# 预期：az1 变为 2

# 在 Prometheus UI 验证告警（< 12 时触发，测试环境阈值需临时调低！）
# ⚠️  测试环境每 AZ 只有 3 个 broker，告警阈值 < 12 永远不会触发
# 临时修改告警规则为 yig_kafka_broker_online_by_az < 3，热重载后测试
```

> **注意**：生产告警阈值 `< 12` 对测试环境（每 AZ 3 个）不适用，测试时临时改为 `< 3`。

**恢复：**

```bash
ssh root@172.25.42.241 "systemctl start kafka"
# 等待 30s，确认 az1 恢复为 3
```

---

### M5/M6 — AZ 副本合规

**目标指标：** `yig_kafka_partition_replication_factor`, `yig_kafka_partition_az_spread_ok`, `yig_kafka_partition_isr_count`, `yig_kafka_topic_min_insync_replicas`

#### 子测试 1：RF 不合规（M5）

BillingTopic（RF=1）启动后即可触发，无需制造。

```bash
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_replication_factor{.*BillingTopic'
# 预期：值为 1（!=3，触发 YigKafkaReplicaNotCompliant）
```

创建专用测试 topic（RF=1，明确标记为测试）：

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --create --topic test-az-rf1 \
  --partitions 3 --replication-factor 1

# 30s 后验证
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_replication_factor{.*test-az-rf1'
# 预期：所有 partition 值为 1
```

#### 子测试 2：AZ 分布不合规（M6）

创建测试 topic，副本故意分配在同一个 AZ（手动指定 replica assignment）：

```bash
# 假设 az1 的 broker ID 为 3,X,Y
# 副本全在 AZ1 的 3 个 broker 上，不跨 AZ
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --create --topic test-az-spread \
  --replica-assignment "az1_id1:az1_id2:az1_id3"
  # 格式示例：--replica-assignment 3:5:8  (partition 0 的 3 个副本)

# 验证 az_spread_ok = 0
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_az_spread_ok{.*test-az-spread'
```

#### 子测试 3：min.insync.replicas 配置不合规

```bash
# 创建 min.insync.replicas=1 的 topic（应为 2）
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --create --topic test-az-minisr \
  --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=1

# 验证
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_topic_min_insync_replicas{.*test-az-minisr'
# 预期：值为 1（!=2，触发 YigKafkaMinIsrConfigWrong）
```

#### 子测试 4：ISR 缩容（已有现成场景）

statTopic 的 broker 7 已不在 ISR（Replicas:7,3,5 但 ISR:5,3）：

```bash
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_isr_count{.*statTopic'
# 预期：值为 2

curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_replication_factor{.*statTopic'
# 预期：值为 3

# isr_count(2) < replication_factor(3) → 触发 YigKafkaIsrShrunk ✓
```

**清理测试 topic：**

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.25.42.241:9092 \
  --delete --topic test-az-rf1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.25.42.241:9092 \
  --delete --topic test-az-spread
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.25.42.241:9092 \
  --delete --topic test-az-minisr
```

---

### M8 — Partition 无 Leader

**目标指标：** `yig_kafka_partition_offline`

**正常状态验证（无损）：**

```bash
curl -s http://172.25.42.247:9308/metrics \
  | grep "yig_kafka_partition_offline" | grep " 1$"
# 预期：无任何输出（所有 partition 都有 leader）
```

**破坏性测试（让某个 partition 失去 leader）：**

> 前提：需要一个 RF=3 但 partition 只分布在某几台 broker 上的 topic，同时关停这些 broker 才能让 leader 丢失。
> 选用 loggingTopic（partition 0 replicas: 5,7,3；partition 1 replicas: 8,3,5）

策略：停掉 partition 0 的所有 ISR broker（broker 5 和 3），使 partition 0 无 leader：

```bash
# 确认 broker 5 和 broker 3 的 IP（从 1.1 步骤结果查）
# 假设 broker 3 = 172.25.42.243，broker 5 = 172.25.42.241（需根据实际填写）

ssh root@<broker3_ip> "systemctl stop kafka"
ssh root@<broker5_ip> "systemctl stop kafka"

# 等待 30s
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_offline{.*loggingTopic.*partition="0"'
# 预期：值为 1（触发 YigKafkaPartitionOffline 告警）
```

**恢复（重要！）：**

```bash
ssh root@<broker3_ip> "systemctl start kafka"
ssh root@<broker5_ip> "systemctl start kafka"

# 等待 broker 重新加入（约 30-60s），确认恢复
curl -s http://172.25.42.247:9308/metrics \
  | grep 'yig_kafka_partition_offline{.*loggingTopic.*partition="0"'
# 预期：值恢复为 0
```

> **注意**：停 broker 前记录哪些 broker 停了，恢复时全部启回。

---

### M1 — 单 Partition LAG 过高

**目标指标：** `kafka_consumergroup_lag`（原生指标）

```bash
# 创建测试 topic
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --create --topic test-lag \
  --partitions 1 --replication-factor 3

# 创建 consumer group（只注册，不实际消费）
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --group test-lag-group \
  --topic test-lag \
  --reset-offsets --to-earliest --execute 2>/dev/null || true

# 快速生产 10001 条消息制造 lag
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic test-lag \
  --num-records 10001 \
  --record-size 100 \
  --throughput -1 \
  --producer-props bootstrap.servers=172.25.42.241:9092

# 等 2 分钟（告警 for: 2m），在 Prometheus 验证
# 查询：kafka_consumergroup_lag{consumergroup="test-lag-group"}
```

预期：Prometheus 中 `kafka_consumergroup_lag` > 10000，持续 2 分钟后触发 `YigKafkaPartitionLagHigh`。

**清理：**

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.25.42.241:9092 \
  --delete --topic test-lag
```

---

### M2 — Topic 总 LAG 过高

M1 的扩展版，生产 100001 条消息：

```bash
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic test-lag \
  --num-records 100001 \
  --record-size 100 \
  --throughput -1 \
  --producer-props bootstrap.servers=172.25.42.241:9092
```

预期：`kafka_consumergroup_lag_sum > 100000`，立即触发 `YigKafkaTopicLagHigh`（for: 0m）。

---

### M3 — 消费者卡死

```bash
# 创建 topic
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --create --topic test-stuck \
  --partitions 1 --replication-factor 3

# 在后台启动 consumer（先让它注册 offset）
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 172.25.42.241:9092 \
  --group test-stuck-group \
  --topic test-stuck &
CONSUMER_PID=$!

# 发几条消息让 offset 前进
echo "msg1" | /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 172.25.42.241:9092 --topic test-stuck
sleep 2

# 暂停 consumer（模拟卡死）
kill -STOP $CONSUMER_PID

# 继续生产消息（让 log-end-offset 继续增长）
for i in $(seq 1 20); do
  echo "msg$i" | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server 172.25.42.241:9092 --topic test-stuck
  sleep 3
done
# 约 2 分钟后，在 Prometheus 验证：
# increase(kafka_consumergroup_current_offset{group="test-stuck-group"}[2m]) == 0
# 且 increase(kafka_topic_partition_current_offset{topic="test-stuck"}[2m]) > 0
# → 触发 YigKafkaConsumerStuck
```

**恢复：**

```bash
kill -CONT $CONSUMER_PID   # 恢复 consumer
kill $CONSUMER_PID         # 停止 consumer
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.25.42.241:9092 \
  --delete --topic test-stuck
```

---

### M4 — 生产者停写

#### 子测试 1：高流量 topic 停写

```bash
# 创建匹配 M4 规则的 topic（loggingTopic 已存在可直接复用）
# 先向 loggingTopic 写几条，然后停 5 分钟，观察告警

# 确认当前 loggingTopic 有写入（查看 offset）
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list 172.25.42.241:9092 --topic loggingTopic --time -1

# 等待 5 分钟不写入，然后在 Prometheus 验证：
# increase(kafka_topic_partition_current_offset{topic="loggingTopic"}[5m]) == 0
# → 触发 YigKafkaProducerStuckHighTraffic
```

> **注意**：loggingTopic 如果有真实业务写入则跳过此测试，改用测试 topic 并修改告警 expr 加入 topic 名。

#### 子测试 2：GC topic 中途挂

```bash
# 先写一些消息到 yig_gc_objs（trigger 10min 有写入条件）
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic yig_gc_objs \
  --num-records 100 \
  --record-size 100 \
  --throughput 10 \
  --producer-props bootstrap.servers=172.25.42.241:9092

# 停止写入，等待 5 分钟
# 在 Prometheus 验证：
# increase(yig_gc_objs[10m]) > 0 AND increase(yig_gc_objs[5m]) == 0
# → 触发 YigKafkaGcTopicStuck
```

---

## 四、验收检查表

| M | 告警名 | 触发方式 | 预期结果 | 通过 |
|---|--------|----------|----------|------|
| M1 | YigKafkaPartitionLagHigh | 生产 10001 条，不消费 2min | lag > 10000 持续 2min | ☐ |
| M2 | YigKafkaTopicLagHigh | 生产 100001 条，不消费 | lag_sum > 100000 | ☐ |
| M3 | YigKafkaConsumerStuck | consumer 暂停，producer 继续 | offset 2min 不动 | ☐ |
| M4-1 | YigKafkaProducerStuckHighTraffic | loggingTopic 5min 无写入 | current_offset 5min 不动 | ☐ |
| M4-2 | YigKafkaGcTopicStuck | yig_gc_objs 先写后停 5min | 10min 有，5min 无 | ☐ |
| M5 | YigKafkaReplicaNotCompliant | BillingTopic RF=1（已有） | RF != 3 | ☐ |
| M6-1 | YigKafkaReplicaNotCompliant | test-az-spread 副本全在一个 AZ | az_spread_ok=0 | ☐ |
| M6-2 | YigKafkaMinIsrConfigWrong | test-az-minisr min.isr=1 | min_isr != 2 | ☐ |
| M6-3 | YigKafkaIsrShrunk | statTopic 已有缩容（已有） | isr_count < rf | ☐ |
| M7 | YigKafkaAzBrokerLow | 停 AZ1 一个 broker | az1 count < 3（需调低阈值） | ☐ |
| M8 | YigKafkaPartitionOffline | 停 loggingTopic leader 的所有 ISR broker | offline=1 | ☐ |

---

## 五、注意事项

1. **M7 告警阈值**：生产阈值 `< 12` 对测试环境（每 AZ 3 个 broker）无效，**测试时临时改为 `< 3`**，测试完改回。
2. **broker 停止顺序**：M8 测试停 broker 时，先确认 loggingTopic partition 0 的 Leader broker 和所有 ISR broker 的 IP（从 kafka-topics.sh --describe 输出对照 1.1 的映射表）。
3. **测试环境复用**：结束后删除所有 test-* topic，告警阈值改回生产值，docker-compose 关停或保留（看是否需要长期监控测试环境）。
4. **发现的现存问题**：BillingTopic RF=1 和 statTopic ISR 缩容是**真实问题**，测试结束后需要反馈给集群维护同学。
