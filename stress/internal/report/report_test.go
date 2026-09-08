package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"redis-stress/internal/config"
	"redis-stress/internal/metrics"
	"redis-stress/internal/sampler"
	"redis-stress/internal/workload"
)

func fixture() Result {
	t0 := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	pct := func(p50, p99 float64) metrics.Percentiles {
		return metrics.Percentiles{Values: map[string]float64{"p50": p50, "p99": p99, "p99.9": p99 * 2}, Max: p99 * 3, Mean: p50, Count: 10}
	}
	client := metrics.ClientResult{
		Start: t0, End: t0.Add(2 * time.Second),
		Snapshots: []metrics.Snapshot{
			{At: t0.Add(time.Second), Elapsed: 1, Seconds: 1,
				Ops:     map[workload.Op]int64{workload.OpGet: 800, workload.OpSet: 200},
				Errors:  map[metrics.ErrorClass]int64{metrics.ErrTimeout: 1},
				Latency: map[workload.Op]metrics.Percentiles{workload.OpGet: pct(500, 2000), workload.OpSet: pct(600, 2500), metrics.OpAll: pct(520, 2100)}},
			{At: t0.Add(2 * time.Second), Elapsed: 2, Seconds: 1,
				Ops:     map[workload.Op]int64{workload.OpGet: 900, workload.OpSet: 100},
				Errors:  map[metrics.ErrorClass]int64{},
				Latency: map[workload.Op]metrics.Percentiles{workload.OpGet: pct(400, 1800), workload.OpSet: pct(500, 2200), metrics.OpAll: pct(420, 1900)}},
		},
		Histogram:   []metrics.HistBucket{{UpperMicros: 1000, Count: 1500}, {UpperMicros: 5000, Count: 500}},
		TotalOps:    map[workload.Op]int64{workload.OpGet: 1700, workload.OpSet: 300},
		TotalErrors: map[metrics.ErrorClass]int64{metrics.ErrTimeout: 1},
		Overall:     map[workload.Op]metrics.Percentiles{workload.OpGet: pct(450, 1900), workload.OpSet: pct(550, 2400)},
		OverallAll:  pct(470, 2000),
	}
	samples := []sampler.Sample{
		{At: t0, NodeID: "10.0.0.1:6379", Addr: "10.0.0.1:6379", Fields: sampler.ServerFields{Role: "master", UsedMemory: 1 << 20, OpsPerSec: 500, ConnectedClients: 50, KeyspaceHits: 100, KeyspaceMisses: 0, Keys: 1000}},
		{At: t0.Add(2 * time.Second), NodeID: "10.0.0.1:6379", Addr: "10.0.0.1:6379", Fields: sampler.ServerFields{Role: "master", UsedMemory: 2 << 20, OpsPerSec: 1000, ConnectedClients: 50, KeyspaceHits: 190, KeyspaceMisses: 10, Keys: 1000}},
		{At: t0.Add(-time.Minute), NodeID: "old", Addr: "old"},
	}
	meta := Meta{Title: "fixture <run>", Version: "test", Start: t0, End: t0.Add(2 * time.Second), DurationS: 2, PreloadS: 0.5, ExitReason: "completed", GeneratedAt: t0}
	return Build(meta, config.Default(), client, samples)
}

func TestBuildSummary(t *testing.T) {
	r := fixture()
	s := r.Summary
	if s.TotalOps != 2000 || s.MeanOpsPerSec != 1000 || s.TotalErrors != 1 {
		t.Errorf("summary counts = %+v", s)
	}
	if s.P50Micros != 470 || s.P99Micros != 2000 || s.P999Micros != 4000 || s.MaxMicros != 6000 {
		t.Errorf("summary latency = %+v", s)
	}
	if s.HitRatio < 0.89 || s.HitRatio > 0.91 {
		t.Errorf("hit ratio = %v, want 0.9", s.HitRatio)
	}
	if len(r.Server) != 1 || r.Server[0].Role != "master" || len(r.Server[0].Samples) != 2 {
		t.Errorf("server series = %+v (samples before start must be dropped)", r.Server)
	}
}

func TestRenderIsSelfContained(t *testing.T) {
	html, err := Render(fixture())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	s := string(html)
	if strings.Contains(s, "<script src=") || strings.Contains(s, "<link ") {
		t.Fatal("report references external assets")
	}
	if n := strings.Count(s, "<canvas"); n != 9 {
		t.Errorf("canvas count = %d, want 9", n)
	}
	if !strings.Contains(s, "fixture &lt;run&gt;") {
		t.Error("title not HTML-escaped")
	}
	if !strings.Contains(s, "Chart.js") {
		t.Error("Chart.js not embedded")
	}
}

func TestWriteAndJSONRoundTrip(t *testing.T) {
	dir := t.TempDir()
	r := fixture()
	if err := Write(dir, r); err != nil {
		t.Fatalf("Write: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var back Result
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Summary != r.Summary || back.Meta.Title != r.Meta.Title || len(back.Client.Snapshots) != 2 {
		t.Errorf("round trip mismatch: %+v", back.Summary)
	}
	if _, err := os.Stat(filepath.Join(dir, "report.html")); err != nil {
		t.Fatal("report.html missing")
	}
}

func TestHitRatioIgnoresFailedSamples(t *testing.T) {
	// A sampler error leaves Fields zero-valued; a failed edge sample must
	// not drag the keyspace delta negative or to zero.
	t0 := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	node := "10.0.0.1:6379"
	samples := []sampler.Sample{
		{At: t0, NodeID: node, Addr: node, Err: "dial tcp: connection refused"},
		{At: t0.Add(time.Second), NodeID: node, Addr: node, Fields: sampler.ServerFields{KeyspaceHits: 100, KeyspaceMisses: 0}},
		{At: t0.Add(2 * time.Second), NodeID: node, Addr: node, Fields: sampler.ServerFields{KeyspaceHits: 190, KeyspaceMisses: 10}},
		{At: t0.Add(3 * time.Second), NodeID: node, Addr: node, Err: "i/o timeout"},
	}
	meta := Meta{Start: t0, End: t0.Add(3 * time.Second), DurationS: 3}
	r := Build(meta, config.Default(), metrics.ClientResult{}, samples)
	if r.Summary.HitRatio < 0.89 || r.Summary.HitRatio > 0.91 {
		t.Errorf("hit ratio = %v, want 0.9 from the two valid samples", r.Summary.HitRatio)
	}
}
