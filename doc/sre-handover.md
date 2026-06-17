# Kafka Exporter 升级交接说明

- 作者：lifc
- 日期：2026-06-17
- 接收方：SRE

---

## 一、背景

当前生产环境（meda0004）运行的是开源 `danielqsj/kafka-exporter`，它缺少以下几项监控能力：

| 监控项 | 问题 |
|--------|------|
| M5/M6 — 副本跨 AZ 合规 | 原 exporter 只暴露副本数量，不暴露副本所在 broker ID，无法判断三 AZ 覆盖 |
| M7 — 分 AZ broker 在线数 | 可用 Prometheus relabel 勉强实现，但维护成本高且易漏改 |
| M8 — Partition 无 Leader | Partition offline 时原 exporter 直接丢弃指标（不报 -1，整条时序消失），PromQL 无法可靠探测 |

本次在 danielqsj 原始代码基础上自研扩展，新增 `yig_kafka_*` 系列指标，**替换原有镜像，端口和 Prometheus scrape 配置不变**。

---

## 二、变更内容

### 2.1 镜像替换

| | 变更前 | 变更后 |
|-|--------|--------|
| 镜像 | `danielqsj/kafka-exporter:latest` | `kafka-exporter:feature-3az-moniter-exporter`（自研构建） |
| 端口 | 9308 | 9308（不变） |
| 原有 flag | 不变 | 不变 |
| 新增 flag | — | `--az.broker-map`（见下） |

### 2.2 新增启动参数

新增一个 flag，用于告诉 exporter 每个 broker ID 属于哪个 AZ：

```
--az.broker-map="ac=27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29|yj=11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10|lf=39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2"
```

**不传此参数时，所有 `yig_kafka_*` 指标不上报，与原有行为完全兼容。**

### 2.3 新增 Prometheus 告警规则

新增告警规则文件，覆盖 M1–M8 全部监控项（M1–M4 基于原有指标，M5–M8 基于新增 `yig_kafka_*` 指标）。

---

## 三、不需要修改的内容

- **Prometheus scrape 配置**：无需改动，仍然抓 `172.108.16.17:9308`
- **原有告警规则**：如果已有 M1–M4 规则，可保留或用新文件覆盖（内容一致）
- **Kafka 集群**：无任何改动

---

## 四、部署步骤

### Step 1：传送镜像到 meda0004

在本地开发机执行：

```bash
# 导出镜像
docker save kafka-exporter:feature-3az-moniter-exporter | gzip > kafka-exporter-3az.tar.gz

# 传到 meda0004
scp kafka-exporter-3az.tar.gz root@172.108.16.17:/root/
```

### Step 2：在 meda0004 加载镜像并切换

```bash
ssh root@172.108.16.17

# 加载镜像
docker load < /root/kafka-exporter-3az.tar.gz

# 进入 docker-compose 所在目录（当前应为 danielqsj 的 compose 文件）
cd <compose目录>

# 停止旧容器
docker-compose down
```

用以下内容替换（或新建）`docker-compose.yml`：

```yaml
version: '3'
services:
  kafka-exporter:
    image: kafka-exporter:feature-3az-moniter-exporter
    container_name: kafka-exporter
    restart: always
    ports:
      - "9308:9308"
    command:
      - "--kafka.server=172.108.16.17:9092"
      - "--kafka.server=172.108.16.18:9092"
      - "--kafka.server=172.108.16.19:9092"
      - "--kafka.server=172.108.20.17:9092"
      - "--kafka.server=172.108.20.18:9092"
      - "--kafka.server=172.108.20.19:9092"
      - "--kafka.server=172.108.24.17:9092"
      - "--kafka.server=172.108.24.18:9092"
      - "--kafka.server=172.108.24.19:9092"
      - "--az.broker-map=ac=27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29|yj=11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10|lf=39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2"
```

```bash
# 启动新容器
docker-compose up -d
```

### Step 3：验证 exporter 正常运行

```bash
# 检查容器状态
docker ps | grep kafka-exporter

# 确认新指标已上报（任意一个即可）
curl -s http://172.108.16.17:9308/metrics | grep yig_kafka_broker_online_by_az
```

预期输出类似：
```
yig_kafka_broker_online_by_az{az="ac"} 16
yig_kafka_broker_online_by_az{az="yj"} 16
yig_kafka_broker_online_by_az{az="lf"} 16
```

### Step 4：加载告警规则

将 `doc/alert_rules/yig_kafka_alerts.yml` 复制到 Prometheus 规则目录，然后热重载：

```bash
# 复制规则文件（路径按实际 Prometheus 配置调整）
cp yig_kafka_alerts.yml /etc/prometheus/rules/

# 热重载（无需重启 Prometheus）
curl -X POST http://<prometheus-addr>:9090/-/reload
```

在 Prometheus UI 的 Alerts 页面确认 M1–M8 规则已加载且状态为 inactive（正常时无告警触发）。

---

## 五、回滚方案

如果新镜像出现问题，把 `docker-compose.yml` 中 `image` 改回 `danielqsj/kafka-exporter:latest` 并重启即可：

```bash
docker-compose down
# 修改 image 为 danielqsj/kafka-exporter:latest，删除 --az.broker-map 行
docker-compose up -d
```

所有 `yig_kafka_*` 指标消失，原有指标和告警不受影响。
