package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"redis-stress/internal/config"
	"redis-stress/internal/workload"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	tests := []struct {
		err  error
		want ErrorClass
	}{
		{context.DeadlineExceeded, ErrTimeout},
		{timeoutErr{}, ErrTimeout},
		{redis.ErrPoolTimeout, ErrTimeout},
		{io.EOF, ErrConnection},
		{syscall.ECONNREFUSED, ErrConnection},
		{&net.OpError{Op: "dial", Err: errors.New("x")}, ErrConnection},
		{errors.New("MOVED 1234 10.0.0.1:6379"), ErrCluster},
		{errors.New("CLUSTERDOWN The cluster is down"), ErrCluster},
		{errors.New("OOM command not allowed when used memory > 'maxmemory'."), ErrOOM},
		{errors.New("WRONGTYPE Operation against a key"), ErrOther},
	}
	for _, tt := range tests {
		if got := Classify(tt.err); got != tt.want {
			t.Errorf("Classify(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
	if Classify(nil) != "" {
		t.Error("Classify(nil) must be empty")
	}
}

func newTestCollector(workers int) *Collector {
	cfg := config.Default().Metrics
	cfg.Percentiles = []float64{50, 99}
	return NewCollector(cfg, workers)
}

func TestTickMergesWorkersAndResets(t *testing.T) {
	c := newTestCollector(2)
	t0 := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	c.Enable(t0)
	for i := 1; i <= 100; i++ {
		c.Recorder(0).Record(workload.OpGet, time.Duration(i)*time.Millisecond, nil)
	}
	c.Recorder(1).Record(workload.OpSet, 5*time.Millisecond, nil)
	c.Recorder(1).Record(workload.OpSet, 0, errors.New("OOM"))

	if abort := c.Tick(t0.Add(time.Second)); abort {
		t.Fatal("unexpected abort")
	}
	res := c.Result()
	if len(res.Snapshots) != 1 {
		t.Fatalf("snapshots = %d", len(res.Snapshots))
	}
	s := res.Snapshots[0]
	if s.Ops[workload.OpGet] != 100 || s.Ops[workload.OpSet] != 1 || s.Errors[ErrOOM] != 1 {
		t.Errorf("counts wrong: ops=%v errs=%v", s.Ops, s.Errors)
	}
	p50 := s.Latency[workload.OpGet].Values["p50"]
	if p50 < 49_000 || p50 > 51_000 {
		t.Errorf("GET p50 = %v µs, want ~50000", p50)
	}
	if s.Latency[OpAll].Count != 101 {
		t.Errorf("ALL count = %d, want 101", s.Latency[OpAll].Count)
	}
	if s.Seconds != 1 {
		t.Errorf("Seconds = %v, want 1", s.Seconds)
	}
	if res.TotalOps[workload.OpGet] != 100 || res.TotalErrors[ErrOOM] != 1 {
		t.Errorf("totals wrong: %v %v", res.TotalOps, res.TotalErrors)
	}
	if res.OverallAll.Max < 99_000 {
		t.Errorf("overall max = %v", res.OverallAll.Max)
	}

	// Counters must be reset after a tick.
	c2 := newTestCollector(1)
	c2.Enable(t0)
	c2.Recorder(0).Record(workload.OpGet, time.Millisecond, nil)
	c2.Tick(t0.Add(time.Second))
	c2.Tick(t0.Add(2 * time.Second))
	if got := c2.Result().Snapshots[1].Ops[workload.OpGet]; got != 0 {
		t.Errorf("second snapshot ops = %d, want 0", got)
	}
}

func TestTickBeforeEnableDiscards(t *testing.T) {
	c := newTestCollector(1)
	t0 := time.Now()
	c.Recorder(0).Record(workload.OpGet, time.Millisecond, nil)
	c.Tick(t0)
	c.Enable(t0)
	c.Tick(t0.Add(time.Second))
	res := c.Result()
	if len(res.Snapshots) != 1 || res.TotalOps[workload.OpGet] != 0 {
		t.Fatalf("warmup data leaked: %+v", res)
	}
}

func TestAbortAfterConsecutiveEmptyIntervals(t *testing.T) {
	c := newTestCollector(1)
	now := time.Now()
	c.Enable(now)
	for i := 1; i <= AbortAfterEmptyIntervals; i++ {
		c.Recorder(0).Record(workload.OpGet, 0, io.EOF)
		abort := c.Tick(now.Add(time.Duration(i) * time.Second))
		if i < AbortAfterEmptyIntervals && abort {
			t.Fatalf("aborted early at interval %d", i)
		}
		if i == AbortAfterEmptyIntervals && !abort {
			t.Fatalf("did not abort at interval %d", i)
		}
	}
}

func TestHistogramBucketsCoverAllSamples(t *testing.T) {
	c := newTestCollector(1)
	t0 := time.Now()
	c.Enable(t0)
	for _, d := range []time.Duration{50 * time.Microsecond, 3 * time.Millisecond, 2 * time.Second} {
		c.Recorder(0).Record(workload.OpSet, d, nil)
	}
	c.Tick(t0.Add(time.Second))
	var total int64
	for _, b := range c.Result().Histogram {
		total += b.Count
	}
	if total != 3 {
		t.Fatalf("bucket total = %d, want 3", total)
	}
}

func TestTickIgnoresDegenerateInterval(t *testing.T) {
	c := newTestCollector(1)
	t0 := time.Now()
	c.Enable(t0)
	c.Recorder(0).Record(workload.OpGet, time.Millisecond, nil)
	if c.Tick(t0.Add(5 * time.Millisecond)) {
		t.Fatal("unexpected abort")
	}
	c.Tick(t0.Add(time.Second))
	res := c.Result()
	if len(res.Snapshots) != 1 {
		t.Fatalf("snapshots = %d, want 1 (degenerate tick must be skipped)", len(res.Snapshots))
	}
	if res.Snapshots[0].Ops[workload.OpGet] != 1 {
		t.Fatalf("op recorded before the skipped tick was lost")
	}
}

func TestFlushBypassesDebounceForTrailingOps(t *testing.T) {
	c := newTestCollector(1)
	t0 := time.Now()
	c.Enable(t0)
	c.Recorder(0).Record(workload.OpGet, time.Millisecond, nil)
	c.Tick(t0.Add(time.Second))
	// Worker records one more op 5ms after the tick (< Interval/10 debounce).
	c.Recorder(0).Record(workload.OpSet, 2*time.Millisecond, nil)
	c.Flush(t0.Add(1005 * time.Millisecond))
	res := c.Result()
	if res.TotalOps[workload.OpSet] != 1 {
		t.Fatalf("Flush dropped trailing op: %+v", res.TotalOps)
	}
	if len(res.Snapshots) != 1 || res.Snapshots[0].Ops[workload.OpSet] != 1 {
		t.Fatalf("expected short Flush to fold into the last snapshot, got %d snapshots: %+v", len(res.Snapshots), res.Snapshots)
	}
}
