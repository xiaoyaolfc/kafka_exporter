# M5-M8 AZ Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend kafka_exporter with 7 new `yig_kafka_*` metrics covering M5/M6 (AZ replica compliance), M7 (per-AZ broker count), and M8 (partition offline detection).

**Architecture:** New file `az_metrics.go` holds all new descriptors and collection methods as methods on the existing `Exporter` struct. `kafka_exporter.go` receives 8 minimal additions: two new struct fields, one new kafkaOpts field, one CLI flag, one `initAZMetrics()` call in `setup()`, descriptor additions in `Describe()`, parse logic in `NewExporter()`, and wiring in `collect()` / `getTopicMetrics()`.

**Tech Stack:** Go 1.26, IBM/sarama v1.47.0, prometheus/client_golang, prometheus/client_model v0.6.1 (for tests)

---

## File Structure

| File | Action | What it does |
|------|--------|-------------|
| `az_metrics.go` | Create | `initAZMetrics()`, `parseAZBrokerMap()`, `collectAZBrokerMetrics()`, `collectTopicMinISR()`, `emitPartitionAZMetrics()` + all 6 yig metric descriptors |
| `az_metrics_test.go` | Create | Unit tests for `parseAZBrokerMap` and `emitPartitionAZMetrics` (no Kafka required) |
| `kafka_exporter.go` | Modify | 8 targeted additions described in Tasks 2–3 |

---

## Task 1: Create `az_metrics.go` and `az_metrics_test.go`

**Files:**
- Create: `az_metrics.go`
- Create: `az_metrics_test.go`

- [ ] **Step 1: Write the failing tests**

Create `az_metrics_test.go`:

```go
package main

import (
	"testing"
	"reflect"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
)

// --- parseAZBrokerMap tests ---

func TestParseAZBrokerMap_Empty(t *testing.T) {
	got := parseAZBrokerMap("")
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

func TestParseAZBrokerMap_Valid(t *testing.T) {
	got := parseAZBrokerMap("ac=1,2|yj=3,4|lf=5,6")
	want := map[int32]string{
		1: "ac", 2: "ac",
		3: "yj", 4: "yj",
		5: "lf", 6: "lf",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseAZBrokerMap_Spaces(t *testing.T) {
	got := parseAZBrokerMap("ac= 1 , 2 | yj= 3")
	want := map[int32]string{1: "ac", 2: "ac", 3: "yj"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseAZBrokerMap_Production(t *testing.T) {
	s := "ac=27,5,31,28,26,32,30,4,24,6,23,25,34,35,33,29|yj=11,21,22,13,8,19,16,12,18,14,7,9,20,15,17,10|lf=39,42,37,45,48,43,44,1,47,3,40,38,41,36,46,2"
	got := parseAZBrokerMap(s)
	if len(got) != 48 {
		t.Errorf("expected 48 entries, got %d", len(got))
	}
	if got[27] != "ac" {
		t.Errorf("broker 27 should be ac, got %q", got[27])
	}
	if got[11] != "yj" {
		t.Errorf("broker 11 should be yj, got %q", got[11])
	}
	if got[39] != "lf" {
		t.Errorf("broker 39 should be lf, got %q", got[39])
	}
}

// --- emitPartitionAZMetrics tests ---

func TestEmitPartitionAZMetrics_AllAZsCovered(t *testing.T) {
	initAZMetrics(nil)
	e := &Exporter{
		brokerAZ: map[int32]string{1: "ac", 2: "yj", 3: "lf"},
		numAZs:   3,
	}
	ch := make(chan prometheus.Metric, 10)
	// replicas span ac, yj, lf → spread = 1
	e.emitPartitionAZMetrics(ch, "testTopic", "0", []int32{1, 2, 3}, []int32{1, 2, 3})
	close(ch)

	count := 0
	for m := range ch {
		count++
		if m.Desc() == yigPartitionAZSpreadOK {
			var dm dto.Metric
			_ = m.Write(&dm)
			if dm.GetGauge().GetValue() != 1 {
				t.Errorf("expected az_spread_ok=1, got %v", dm.GetGauge().GetValue())
			}
		}
	}
	if count != 3 { // RF, az_spread, ISR
		t.Errorf("expected 3 metrics, got %d", count)
	}
}

func TestEmitPartitionAZMetrics_AZSpreadNotOK(t *testing.T) {
	initAZMetrics(nil)
	// brokers 1 and 2 both map to "ac", broker 3 maps to "yj" — only 2 distinct AZs
	e := &Exporter{
		brokerAZ: map[int32]string{1: "ac", 2: "ac", 3: "yj"},
		numAZs:   3,
	}
	ch := make(chan prometheus.Metric, 10)
	e.emitPartitionAZMetrics(ch, "testTopic", "0", []int32{1, 2, 3}, []int32{1, 2, 3})
	close(ch)

	found := false
	for m := range ch {
		if m.Desc() != yigPartitionAZSpreadOK {
			continue
		}
		found = true
		var dm dto.Metric
		_ = m.Write(&dm)
		if dm.GetGauge().GetValue() != 0 {
			t.Errorf("expected az_spread_ok=0 when only 2 AZs covered, got %v", dm.GetGauge().GetValue())
		}
	}
	if !found {
		t.Error("yigPartitionAZSpreadOK metric not found in output")
	}
}

func TestEmitPartitionAZMetrics_NilReplicas(t *testing.T) {
	initAZMetrics(nil)
	e := &Exporter{
		brokerAZ: map[int32]string{1: "ac"},
		numAZs:   1,
	}
	ch := make(chan prometheus.Metric, 10)
	// nil replicas: RF and az_spread should not be emitted; only ISR count
	e.emitPartitionAZMetrics(ch, "testTopic", "0", nil, []int32{1})
	close(ch)

	count := 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 metric (ISR only) when replicas=nil, got %d", count)
	}
}
```

- [ ] **Step 2: Run tests — verify they fail with compile error**

```bash
cd /home/xiaoyao/iaas/projects/kafka_exporter
go test -run "TestParseAZBrokerMap|TestEmitPartitionAZMetrics" -v .
```

Expected: compile error — `undefined: parseAZBrokerMap`, `undefined: initAZMetrics`, etc.

- [ ] **Step 3: Create `az_metrics.go`**

Create `/home/xiaoyao/iaas/projects/kafka_exporter/az_metrics.go`:

```go
package main

import (
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
)

var (
	yigBrokerOnlineByAZ    *prometheus.Desc
	yigPartitionOffline    *prometheus.Desc
	yigPartitionAZSpreadOK *prometheus.Desc
	yigPartitionRF         *prometheus.Desc
	yigPartitionISRCount   *prometheus.Desc
	yigTopicMinISR         *prometheus.Desc
)

// initAZMetrics initialises all yig_kafka_* descriptors with the same constant labels
// as the existing kafka_* metrics. Must be called from setup() after labels is built.
func initAZMetrics(labels map[string]string) {
	yigBrokerOnlineByAZ = prometheus.NewDesc(
		"yig_kafka_broker_online_by_az",
		"Number of online Kafka brokers per availability zone.",
		[]string{"az"}, labels,
	)
	yigPartitionOffline = prometheus.NewDesc(
		"yig_kafka_partition_offline",
		"1 if the partition has no leader (offline), 0 if healthy.",
		[]string{"topic", "partition"}, labels,
	)
	yigPartitionAZSpreadOK = prometheus.NewDesc(
		"yig_kafka_partition_az_spread_ok",
		"1 if the partition replicas span all configured AZs, 0 otherwise.",
		[]string{"topic", "partition"}, labels,
	)
	yigPartitionRF = prometheus.NewDesc(
		"yig_kafka_partition_replication_factor",
		"Total number of replicas for the partition.",
		[]string{"topic", "partition"}, labels,
	)
	yigPartitionISRCount = prometheus.NewDesc(
		"yig_kafka_partition_isr_count",
		"Current number of in-sync replicas (runtime value, not Kafka config).",
		[]string{"topic", "partition"}, labels,
	)
	yigTopicMinISR = prometheus.NewDesc(
		"yig_kafka_topic_min_insync_replicas",
		"Configured min.insync.replicas for the topic (Kafka config value, not runtime ISR count).",
		[]string{"topic"}, labels,
	)
}

// parseAZBrokerMap parses the --az.broker-map flag value into a broker ID → AZ name map.
// Format: "az1=id1,id2,...|az2=id3,id4,..."  Leading/trailing spaces around IDs are trimmed.
func parseAZBrokerMap(s string) map[int32]string {
	result := make(map[int32]string)
	if s == "" {
		return result
	}
	for _, group := range strings.Split(s, "|") {
		parts := strings.SplitN(group, "=", 2)
		if len(parts) != 2 {
			continue
		}
		az := strings.TrimSpace(parts[0])
		for _, idStr := range strings.Split(parts[1], ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 32)
			if err != nil {
				klog.Warningf("az.broker-map: invalid broker ID %q in AZ %q: %v", idStr, az, err)
				continue
			}
			result[int32(id)] = az
		}
	}
	return result
}

// collectAZBrokerMetrics emits yig_kafka_broker_online_by_az for each AZ. [M7]
func (e *Exporter) collectAZBrokerMetrics(ch chan<- prometheus.Metric) {
	counts := make(map[string]int)
	for _, b := range e.client.Brokers() {
		if az, ok := e.brokerAZ[b.ID()]; ok {
			counts[az]++
		}
	}
	for az, count := range counts {
		ch <- prometheus.MustNewConstMetric(yigBrokerOnlineByAZ, prometheus.GaugeValue, float64(count), az)
	}
}

// collectTopicMinISR fetches the min.insync.replicas Kafka config value for every topic. [M5/M6]
// Creates a short-lived ClusterAdmin using the same broker addresses as e.client to avoid
// sharing the connection (sarama.ClusterAdmin.Close() also closes a shared client).
func (e *Exporter) collectTopicMinISR() map[string]int64 {
	result := make(map[string]int64)

	addrs := make([]string, 0, len(e.client.Brokers()))
	for _, b := range e.client.Brokers() {
		addrs = append(addrs, b.Addr())
	}
	admin, err := sarama.NewClusterAdmin(addrs, e.client.Config())
	if err != nil {
		klog.Errorf("collectTopicMinISR: cannot create ClusterAdmin: %v", err)
		return result
	}
	defer admin.Close()

	topics, err := e.client.Topics()
	if err != nil {
		klog.Errorf("collectTopicMinISR: cannot list topics: %v", err)
		return result
	}

	for _, topic := range topics {
		entries, err := admin.DescribeConfig(sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        topic,
			ConfigNames: []string{"min.insync.replicas"},
		})
		if err != nil {
			klog.Errorf("collectTopicMinISR: DescribeConfig %s: %v", topic, err)
			continue
		}
		for _, entry := range entries {
			if entry.Name == "min.insync.replicas" {
				if val, err := strconv.ParseInt(entry.Value, 10, 64); err == nil {
					result[topic] = val
				}
				break
			}
		}
	}
	return result
}

// emitPartitionAZMetrics emits partition-level M5/M6 AZ compliance metrics.
// replicas or inSyncReplicas may be nil when the corresponding Sarama call failed;
// those metrics are silently skipped rather than emitting a misleading zero.
func (e *Exporter) emitPartitionAZMetrics(
	ch chan<- prometheus.Metric,
	topic, partStr string,
	replicas []int32,
	inSyncReplicas []int32,
) {
	if replicas != nil {
		ch <- prometheus.MustNewConstMetric(yigPartitionRF, prometheus.GaugeValue, float64(len(replicas)), topic, partStr)

		azSet := make(map[string]struct{})
		for _, brokerID := range replicas {
			if az, ok := e.brokerAZ[brokerID]; ok {
				azSet[az] = struct{}{}
			}
		}
		spread := 0.0
		if len(azSet) >= e.numAZs {
			spread = 1.0
		}
		ch <- prometheus.MustNewConstMetric(yigPartitionAZSpreadOK, prometheus.GaugeValue, spread, topic, partStr)
	}

	if inSyncReplicas != nil {
		ch <- prometheus.MustNewConstMetric(yigPartitionISRCount, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, partStr)
	}
}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
go test -run "TestParseAZBrokerMap|TestEmitPartitionAZMetrics" -v .
```

Expected: all 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add az_metrics.go az_metrics_test.go
git commit -m "feat: add az_metrics.go with yig_kafka_* descriptors and collection logic"
```

---

## Task 2: Modify `kafka_exporter.go` — Static additions (struct, flag, init, Describe, NewExporter)

**Files:**
- Modify: `kafka_exporter.go`

- [ ] **Step 1: Add `brokerAZ` and `numAZs` fields to `Exporter` struct**

In `kafka_exporter.go` at the `Exporter` struct (lines 68–88), add two fields before the closing `}`:

```go
// FIND (line 87–88):
	consumerGroupFetchAll   bool
	groupMetricsTimeout     time.Duration
}

// REPLACE WITH:
	consumerGroupFetchAll   bool
	groupMetricsTimeout     time.Duration
	brokerAZ                map[int32]string // broker ID → AZ name; empty when --az.broker-map not set
	numAZs                  int              // number of distinct AZ names in brokerAZ
}
```

- [ ] **Step 2: Add `azBrokerMap` field to `kafkaOpts` struct**

In the `kafkaOpts` struct (lines 90–129), add before the closing `}`:

```go
// FIND (line 128–129):
	groupMetricsTimeout      string
}

// REPLACE WITH:
	groupMetricsTimeout      string
	azBrokerMap              string
}
```

- [ ] **Step 3: Add `--az.broker-map` flag in `main()`**

In `main()` (around line 916), after the `toFlagStringVar("group.metrics.timeout", ...)` line, add:

```go
toFlagStringVar("az.broker-map", "AZ-aware broker map, format: az1=id1,id2|az2=id3,id4. When empty, yig_kafka_* metrics are not emitted.", "", &opts.azBrokerMap)
```

- [ ] **Step 4: Call `initAZMetrics(labels)` in `setup()`**

In `setup()` (around line 1053), after the `consumergroupMembers = prometheus.NewDesc(...)` block and before the `if logSarama {` line, add:

```go
// FIND:
	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	if logSarama {

// REPLACE WITH:
	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	initAZMetrics(labels)

	if logSarama {
```

- [ ] **Step 5: Add yig descriptors to `Describe()`**

In `Describe()` (lines 391–408), add after the last `ch <- consumergroupLagSum` line:

```go
// FIND:
	ch <- consumergroupLagSum
}

// REPLACE WITH:
	ch <- consumergroupLagSum
	ch <- yigBrokerOnlineByAZ
	ch <- yigPartitionOffline
	ch <- yigPartitionAZSpreadOK
	ch <- yigPartitionRF
	ch <- yigPartitionISRCount
	ch <- yigTopicMinISR
}
```

- [ ] **Step 6: Parse broker map and populate fields in `NewExporter()`**

In `NewExporter()`, add parsing logic before the `return &Exporter{` statement (around line 357) and new fields in the return:

```go
// FIND (the return statement, around line 356):
	klog.V(TRACE).Infoln("Done Init Clients")
	// Init our exporter.
	return &Exporter{

// REPLACE WITH:
	klog.V(TRACE).Infoln("Done Init Clients")
	parsedBrokerAZ := parseAZBrokerMap(opts.azBrokerMap)
	azNameSet := make(map[string]struct{})
	for _, az := range parsedBrokerAZ {
		azNameSet[az] = struct{}{}
	}
	// Init our exporter.
	return &Exporter{
```

Then add the two new fields at the end of the return struct literal, before `}, nil`:

```go
// FIND:
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
		groupMetricsTimeout:     groupMetricsTimeout,
	}, nil

// REPLACE WITH:
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
		groupMetricsTimeout:     groupMetricsTimeout,
		brokerAZ:                parsedBrokerAZ,
		numAZs:                  len(azNameSet),
	}, nil
```

- [ ] **Step 7: Verify it compiles**

```bash
go build .
```

Expected: no errors. Fix any compile errors before proceeding.

- [ ] **Step 8: Commit**

```bash
git add kafka_exporter.go
git commit -m "feat: wire az.broker-map flag and Exporter fields into kafka_exporter.go"
```

---

## Task 3: Modify `kafka_exporter.go` — Wire M7/M8/M5/M6 into `collect()` and `getTopicMetrics()`

**Files:**
- Modify: `kafka_exporter.go`

- [ ] **Step 1: Add M7 collection and minISR fetch in `collect()`**

In `collect()`, after the broker info loop (after the `for _, b := range e.client.Brokers() { ... }` closing brace, around line 469) and before `offset := make(...)`, add:

```go
// FIND:
	for _, b := range e.client.Brokers() {
		ch <- prometheus.MustNewConstMetric(
			clusterBrokerInfo, prometheus.GaugeValue, 1, strconv.Itoa(int(b.ID())), b.Addr(),
		)
	}

	offset := make(map[string]map[int32]int64)

// REPLACE WITH:
	for _, b := range e.client.Brokers() {
		ch <- prometheus.MustNewConstMetric(
			clusterBrokerInfo, prometheus.GaugeValue, 1, strconv.Itoa(int(b.ID())), b.Addr(),
		)
	}

	var minISR map[string]int64
	if len(e.brokerAZ) > 0 {
		e.collectAZBrokerMetrics(ch)
		minISR = e.collectTopicMinISR()
	}

	offset := make(map[string]map[int32]int64)
```

- [ ] **Step 2: Emit topic-level `yig_kafka_topic_min_insync_replicas` before the partition loop**

Inside the `getTopicMetrics` closure, after the `e.mu.Unlock()` call that initialises `offset[topic]` (around line 514) and before `for _, partition := range partitions {`, add:

```go
// FIND:
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		e.mu.Unlock()
		for _, partition := range partitions {

// REPLACE WITH:
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		e.mu.Unlock()
		if len(e.brokerAZ) > 0 {
			if val, ok := minISR[topic]; ok {
				ch <- prometheus.MustNewConstMetric(yigTopicMinISR, prometheus.GaugeValue, float64(val), topic)
			}
		}
		for _, partition := range partitions {
```

- [ ] **Step 3: Replace the leader check block with the M8 gate**

Inside the partition loop, replace the existing leader check (lines 516–523) with the M8 gate that uses `continue` on error and introduces `partStr`:

```go
// FIND (lines 515–523):
		for _, partition := range partitions {
			broker, err := e.client.Leader(topic, partition)
			if err != nil {
				klog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

// REPLACE WITH:
		for _, partition := range partitions {
			partStr := strconv.FormatInt(int64(partition), 10)
			broker, err := e.client.Leader(topic, partition)
			if err != nil {
				klog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
				if len(e.brokerAZ) > 0 {
					ch <- prometheus.MustNewConstMetric(yigPartitionOffline, prometheus.GaugeValue, 1, topic, partStr)
				}
				continue
			}
			ch <- prometheus.MustNewConstMetric(topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, partStr)
			if len(e.brokerAZ) > 0 {
				ch <- prometheus.MustNewConstMetric(yigPartitionOffline, prometheus.GaugeValue, 0, topic, partStr)
			}
```

- [ ] **Step 4: Replace all `strconv.FormatInt(int64(partition), 10)` inside the loop with `partStr`**

After Step 3, the variable `partStr` is declared at the top of the partition loop. Replace the remaining occurrences of `strconv.FormatInt(int64(partition), 10)` within the loop body (lines 533, 542, 551, 560, 566, 570, 576, 580, 596) with `partStr`. Use your editor's find-and-replace scoped to the `for _, partition := range partitions` block.

Verification — after the replacement, no occurrence of `strconv.FormatInt(int64(partition), 10)` should remain inside the partition loop:

```bash
grep -n 'FormatInt(int64(partition)' kafka_exporter.go
```

Expected: no output (all replaced with `partStr`).

- [ ] **Step 5: Add `emitPartitionAZMetrics` call after the `inSyncReplicas` block**

After the existing `inSyncReplicas` if/else block (around line 562) and before the preferred-replica check, add:

```go
// FIND:
			if inSyncReplicas != nil && ... {  ← the inSyncReplicas else block closing brace
			}

			if broker != nil && replicas != nil ...  ← preferred replica check

// INSERT BETWEEN THEM:
			if len(e.brokerAZ) > 0 {
				e.emitPartitionAZMetrics(ch, topic, partStr, replicas, inSyncReplicas)
			}
```

The exact anchor is the closing `}` of the `inSyncReplicas` else block. After that closing brace, insert:

```go
			if len(e.brokerAZ) > 0 {
				e.emitPartitionAZMetrics(ch, topic, partStr, replicas, inSyncReplicas)
			}
```

- [ ] **Step 6: Verify it compiles and existing tests still pass**

```bash
go build .
go test -run "TestParseAZBrokerMap|TestEmitPartitionAZMetrics" -v .
```

Expected: `go build` succeeds, 7 unit tests PASS.

- [ ] **Step 7: Commit**

```bash
git add kafka_exporter.go
git commit -m "feat: wire M5-M8 collection into collect() and getTopicMetrics()"
```

---

## Task 4: Docker build verification

**Files:** none (build artefact only)

- [ ] **Step 1: Build the binary**

```bash
cd /home/xiaoyao/iaas/projects/kafka_exporter
go build -o kafka_exporter .
```

Expected: binary produced, no errors.

- [ ] **Step 2: Build the Docker image**

```bash
make build   # or: CGO_ENABLED=0 GOOS=linux go build -o .build/linux-amd64/kafka_exporter .
docker build --build-arg BIN_DIR=.build/linux-amd64/ -t kafka-exporter:feature-3az-moniter-exporter .
```

Expected: image built successfully.

- [ ] **Step 3: Smoke-test — verify new flags appear in help**

```bash
docker run --rm kafka-exporter:feature-3az-moniter-exporter --help | grep az.broker-map
```

Expected: line containing `--az.broker-map` with the help text.

- [ ] **Step 4: Commit build artefact cleanup and tag**

```bash
rm -f kafka_exporter   # remove local binary (not tracked)
git tag feature/3az-moniter-exporter-ready
```

---

## Prometheus alert rules (reference — not part of Go implementation)

After deploying, add the following to your Prometheus alert rule file. These are provided for reference; apply them to your Prometheus config separately.

```yaml
groups:
  - name: yig_kafka_m5_m8
    rules:
      - alert: YigKafkaReplicaNotCompliant
        expr: yig_kafka_partition_replication_factor != 3 or yig_kafka_partition_az_spread_ok == 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "副本不合规: {{ $labels.topic }}/p{{ $labels.partition }}"

      - alert: YigKafkaIsrShrunk
        expr: yig_kafka_partition_isr_count < yig_kafka_partition_replication_factor
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "ISR缩容: {{ $labels.topic }}/p{{ $labels.partition }} ISR={{ $value }}"

      - alert: YigKafkaMinIsrConfigWrong
        expr: yig_kafka_topic_min_insync_replicas != 2
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "min.insync.replicas配置异常: {{ $labels.topic }} 当前值={{ $value }}，应为2"

      - alert: YigKafkaAzBrokerLow
        expr: yig_kafka_broker_online_by_az < 12
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "AZ {{ $labels.az }} broker 仅剩 {{ $value }}/16"

      - alert: YigKafkaPartitionOffline
        expr: yig_kafka_partition_offline == 1
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Partition offline: {{ $labels.topic }}/p{{ $labels.partition }}"
```
