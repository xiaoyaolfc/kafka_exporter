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
