package workload

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"golang.org/x/time/rate"

	"redis-stress/internal/config"
)

type countingRecorder struct {
	mu   sync.Mutex
	ops  map[Op]int
	errs int
	lat  []time.Duration
}

func (r *countingRecorder) Record(op Op, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ops == nil {
		r.ops = map[Op]int{}
	}
	if err != nil {
		r.errs++
		return
	}
	r.ops[op]++
	r.lat = append(r.lat, latency)
}

func testTarget(addr string) config.Target {
	t := config.Default().Target
	t.Mode = config.ModeStandalone
	t.Addrs = []string{addr}
	t.PasswordEnv = ""
	return t
}

type testLogger struct{ lines []string }

func (l *testLogger) Printf(format string, args ...any) { l.lines = append(l.lines, format) }

func TestPreloadWritesEveryKey(t *testing.T) {
	srv := miniredis.RunT(t)
	client := NewClient(testTarget(srv.Addr()), 4)
	defer client.Close()
	cfg := config.Default().Workload
	cfg.KeySpace = 2500
	cfg.ValueSize = 8
	log := &testLogger{}
	if err := Preload(context.Background(), client, cfg, log); err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if n := len(srv.Keys()); n != 2500 {
		t.Fatalf("keys after preload = %d, want 2500", n)
	}
	if v, _ := srv.Get("stress:2499"); len(v) != 8 {
		t.Fatalf("value size = %d, want 8", len(v))
	}
	if len(log.lines) == 0 {
		t.Error("expected progress log lines")
	}
}

func TestRunRecordsEveryCommand(t *testing.T) {
	for _, pipeline := range []int{1, 4} {
		srv := miniredis.RunT(t)
		client := NewClient(testTarget(srv.Addr()), 4)
		cfg := config.Default().Workload
		cfg.Concurrency = 2
		cfg.Pipeline = pipeline
		cfg.KeySpace = 50
		cfg.ReadRatio = 0.5
		cfg.TTL = 60
		recs := []Recorder{&countingRecorder{}, &countingRecorder{}}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		Run(ctx, client, cfg, recs, nil)
		cancel()
		client.Close()
		total := 0
		for _, r := range recs {
			cr := r.(*countingRecorder)
			total += cr.ops[OpGet] + cr.ops[OpSet]
			if cr.errs != 0 {
				t.Errorf("pipeline=%d: unexpected errors: %d", pipeline, cr.errs)
			}
			if pipeline > 1 && (cr.ops[OpGet]+cr.ops[OpSet])%pipeline != 0 {
				t.Errorf("pipeline=%d: ops %d not a multiple of pipeline", pipeline, cr.ops[OpGet]+cr.ops[OpSet])
			}
		}
		if total == 0 {
			t.Fatalf("pipeline=%d: no ops recorded", pipeline)
		}
		if ttl := srv.TTL("stress:1"); srv.Exists("stress:1") && ttl <= 0 {
			t.Errorf("pipeline=%d: SET did not apply TTL", pipeline)
		}
	}
}

func TestRunHonoursRateLimit(t *testing.T) {
	srv := miniredis.RunT(t)
	client := NewClient(testTarget(srv.Addr()), 4)
	defer client.Close()
	cfg := config.Default().Workload
	cfg.Concurrency = 4
	cfg.KeySpace = 10
	rec := &countingRecorder{}
	recs := []Recorder{rec, rec, rec, rec}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	Run(ctx, client, cfg, recs, rate.NewLimiter(100, 4))
	if n := rec.ops[OpGet] + rec.ops[OpSet]; n > 80 {
		t.Fatalf("rate limit 100/s over 0.5s produced %d ops", n)
	}
}
