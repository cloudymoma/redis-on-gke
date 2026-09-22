package metrics

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"redis-stress/internal/config"
	"redis-stress/internal/workload"
)

// OpAll is the pseudo-op holding latencies of every command type merged.
const OpAll workload.Op = "ALL"

// AbortAfterEmptyIntervals is how many consecutive intervals with errors and
// no successful commands trigger an early abort.
const AbortAfterEmptyIntervals = 10

const (
	minLatencyMicros = 1
	maxLatencyMicros = 60_000_000
	sigFigs          = 3
)

// Percentiles summarises one histogram. Values are keyed "p50", "p99.9" and
// expressed in microseconds.
type Percentiles struct {
	Values map[string]float64 `json:"values"`
	Max    float64            `json:"max"`
	Mean   float64            `json:"mean"`
	Count  int64              `json:"count"`
}

// Snapshot is one collection interval.
type Snapshot struct {
	At      time.Time                   `json:"at"`
	Elapsed float64                     `json:"elapsed_s"`
	Seconds float64                     `json:"seconds"`
	Ops     map[workload.Op]int64       `json:"ops"`
	Errors  map[ErrorClass]int64        `json:"errors"`
	Latency map[workload.Op]Percentiles `json:"latency"`
}

// HistBucket is one bar of the cumulative latency distribution.
type HistBucket struct {
	UpperMicros int64 `json:"upper_us"`
	Count       int64 `json:"count"`
}

// ClientResult is everything the client side measured.
type ClientResult struct {
	Start       time.Time                   `json:"start"`
	End         time.Time                   `json:"end"`
	Snapshots   []Snapshot                  `json:"snapshots"`
	Histogram   []HistBucket                `json:"histogram"`
	TotalOps    map[workload.Op]int64       `json:"total_ops"`
	TotalErrors map[ErrorClass]int64        `json:"total_errors"`
	Overall     map[workload.Op]Percentiles `json:"overall"`
	OverallAll  Percentiles                 `json:"overall_all"`
}

var bucketBoundsMicros = []int64{100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
	100_000, 250_000, 500_000, 1_000_000, math.MaxInt64}

func newHist() *hdrhistogram.Histogram {
	return hdrhistogram.New(minLatencyMicros, maxLatencyMicros, sigFigs)
}

type workerRecorder struct {
	mu   sync.Mutex
	hist map[workload.Op]*hdrhistogram.Histogram
	ops  map[workload.Op]int64
	errs map[ErrorClass]int64
}

func newWorkerRecorder() *workerRecorder {
	return &workerRecorder{
		hist: map[workload.Op]*hdrhistogram.Histogram{workload.OpGet: newHist(), workload.OpSet: newHist()},
		ops:  map[workload.Op]int64{},
		errs: map[ErrorClass]int64{},
	}
}

func (w *workerRecorder) Record(op workload.Op, latency time.Duration, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.errs[Classify(err)]++
		return
	}
	w.ops[op]++
	v := latency.Microseconds()
	if v < minLatencyMicros {
		v = minLatencyMicros
	}
	if v > maxLatencyMicros {
		v = maxLatencyMicros
	}
	_ = w.hist[op].RecordValue(v)
}

// drainInto merges and resets this worker's data. Nil maps discard.
func (w *workerRecorder) drainInto(hists map[workload.Op]*hdrhistogram.Histogram, ops map[workload.Op]int64, errs map[ErrorClass]int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for op, h := range w.hist {
		if hists != nil {
			hists[op].Merge(h)
		}
		h.Reset()
	}
	for op, n := range w.ops {
		if ops != nil {
			ops[op] += n
		}
		w.ops[op] = 0
	}
	for k, n := range w.errs {
		if errs != nil {
			errs[k] += n
		}
		w.errs[k] = 0
	}
}

// Collector owns the per-worker recorders and produces snapshots.
type Collector struct {
	cfg     config.Metrics
	workers []*workerRecorder

	mu            sync.Mutex
	enabled       bool
	start         time.Time
	lastTick      time.Time
	snapshots     []Snapshot
	cumulative    map[workload.Op]*hdrhistogram.Histogram
	cumulativeAll *hdrhistogram.Histogram
	totalOps      map[workload.Op]int64
	totalErrs     map[ErrorClass]int64
	emptyStreak   int
	aborted       bool
	wg            sync.WaitGroup
}

func NewCollector(cfg config.Metrics, workers int) *Collector {
	c := &Collector{
		cfg:           cfg,
		workers:       make([]*workerRecorder, workers),
		cumulative:    map[workload.Op]*hdrhistogram.Histogram{workload.OpGet: newHist(), workload.OpSet: newHist()},
		cumulativeAll: newHist(),
		totalOps:      map[workload.Op]int64{},
		totalErrs:     map[ErrorClass]int64{},
	}
	for i := range c.workers {
		c.workers[i] = newWorkerRecorder()
	}
	return c
}

// Recorder returns the recorder for worker i.
func (c *Collector) Recorder(i int) workload.Recorder { return c.workers[i] }

// Enable discards everything recorded so far (warmup) and starts measuring.
func (c *Collector) Enable(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.discard()
	c.enabled = true
	c.start = now
	c.lastTick = now
}

func (c *Collector) discard() {
	for _, w := range c.workers {
		w.drainInto(nil, nil, nil)
	}
}

// Tick merges all workers into one snapshot. Returns true exactly once when
// the early-abort condition is met.
func (c *Collector) Tick(now time.Time) bool {
	return c.tick(now, false)
}

// Flush waits for the background ticker (if running) and drains any
// observations recorded after the last tick, bypassing the interval debounce.
func (c *Collector) Flush(now time.Time) {
	c.wg.Wait()
	c.tick(now, true)
}

func (c *Collector) tick(now time.Time, final bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled {
		c.discard()
		return false
	}
	short := now.Sub(c.lastTick) < c.cfg.Interval/10
	// A ticker started before Enable can fire almost immediately after it;
	// let that data roll into the next full interval instead of emitting a
	// near-empty snapshot with a meaningless rate.
	if !final && short {
		return false
	}
	interval := map[workload.Op]*hdrhistogram.Histogram{workload.OpGet: newHist(), workload.OpSet: newHist()}
	ops := map[workload.Op]int64{}
	errs := map[ErrorClass]int64{}
	for _, w := range c.workers {
		w.drainInto(interval, ops, errs)
	}
	var totalOps, totalErrs int64
	for _, n := range ops {
		totalOps += n
	}
	for _, n := range errs {
		totalErrs += n
	}
	if final && short && totalOps == 0 && totalErrs == 0 {
		return false
	}

	all := newHist()
	subSeconds := now.Sub(c.lastTick).Seconds()
	c.lastTick = now
	for op, h := range interval {
		c.cumulative[op].Merge(h)
		all.Merge(h)
	}
	c.cumulativeAll.Merge(all)
	for op, n := range ops {
		c.totalOps[op] += n
	}
	for k, n := range errs {
		c.totalErrs[k] += n
	}

	// If a final flush lands inside the debounce window after at least one
	// snapshot already exists, fold its counts and duration into that last
	// snapshot rather than emitting a few-millisecond tail bar with a noisy rate
	// (its Latency percentiles remain from the full interval; cumulative includes all).
	if final && short && len(c.snapshots) > 0 {
		last := &c.snapshots[len(c.snapshots)-1]
		last.At = now
		last.Elapsed = now.Sub(c.start).Seconds()
		last.Seconds += subSeconds
		for op, n := range ops {
			last.Ops[op] += n
		}
		for k, n := range errs {
			last.Errors[k] += n
		}
		return false
	}

	snap := Snapshot{
		At:      now,
		Elapsed: now.Sub(c.start).Seconds(),
		Seconds: subSeconds,
		Ops:     ops,
		Errors:  errs,
		Latency: map[workload.Op]Percentiles{},
	}
	for op, h := range interval {
		snap.Latency[op] = percentilesOf(h, c.cfg.Percentiles)
	}
	snap.Latency[OpAll] = percentilesOf(all, c.cfg.Percentiles)
	c.snapshots = append(c.snapshots, snap)

	if totalOps == 0 && totalErrs > 0 {
		c.emptyStreak++
	} else {
		c.emptyStreak = 0
	}
	if c.emptyStreak >= AbortAfterEmptyIntervals && !c.aborted {
		c.aborted = true
		return true
	}
	return false
}

// Start ticks every cfg.Interval until ctx is done, then takes a final tick.
// onAbort is called once if the early-abort condition is met.
func (c *Collector) Start(ctx context.Context, onAbort func()) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.cfg.Interval)
		defer t.Stop()
		for {
			select {
			case now := <-t.C:
				if c.Tick(now) {
					onAbort()
				}
			case <-ctx.Done():
				c.tick(time.Now(), true)
				return
			}
		}
	}()
}

// Result waits for Start's goroutine (if any) and returns a copy of the data.
func (c *Collector) Result() ClientResult {
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	res := ClientResult{
		Start:       c.start,
		End:         c.lastTick,
		Snapshots:   append([]Snapshot(nil), c.snapshots...),
		Histogram:   buckets(c.cumulativeAll),
		TotalOps:    map[workload.Op]int64{},
		TotalErrors: map[ErrorClass]int64{},
		Overall:     map[workload.Op]Percentiles{},
		OverallAll:  percentilesOf(c.cumulativeAll, c.cfg.Percentiles),
	}
	for op, n := range c.totalOps {
		res.TotalOps[op] = n
	}
	for k, n := range c.totalErrs {
		res.TotalErrors[k] = n
	}
	for op, h := range c.cumulative {
		res.Overall[op] = percentilesOf(h, c.cfg.Percentiles)
	}
	return res
}

func percentilesOf(h *hdrhistogram.Histogram, ps []float64) Percentiles {
	out := Percentiles{Values: map[string]float64{}, Count: h.TotalCount()}
	for _, p := range ps {
		out.Values[pName(p)] = float64(h.ValueAtQuantile(p))
	}
	if out.Count > 0 {
		out.Max = float64(h.Max())
		out.Mean = h.Mean()
	}
	return out
}

func pName(p float64) string { return "p" + strconv.FormatFloat(p, 'f', -1, 64) }

func buckets(h *hdrhistogram.Histogram) []HistBucket {
	out := make([]HistBucket, len(bucketBoundsMicros))
	for i, b := range bucketBoundsMicros {
		out[i].UpperMicros = b
	}
	for _, bar := range h.Distribution() {
		if bar.Count == 0 {
			continue
		}
		for i, b := range bucketBoundsMicros {
			if bar.To <= b {
				out[i].Count += bar.Count
				break
			}
		}
	}
	return out
}
