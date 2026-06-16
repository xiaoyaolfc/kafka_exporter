package main

import (
	"reflect"
	"testing"

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
