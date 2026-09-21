// Command redis-stress runs a configurable GET/SET load test against Redis
// and writes report.html + report.json.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"redis-stress/internal/config"
	"redis-stress/internal/metrics"
	"redis-stress/internal/report"
	"redis-stress/internal/sampler"
	"redis-stress/internal/workload"
)

var version = "dev"

const (
	exitOK        = 0
	exitConfig    = 2
	exitPreflight = 3
	exitAborted   = 4
	exitReport    = 5
)

func main() { os.Exit(run(os.Args[1:], os.Stderr)) }

func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("redis-stress", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "config.yaml", "path to config.yaml")
	outDir := fs.String("out", "out", "directory for report.html and report.json")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Printf("config error: %v", err)
		return exitConfig
	}
	logger.Printf("redis-stress %s: %s", version, cfg.Report.Title)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := workload.NewClient(cfg.Target, cfg.Workload.Concurrency)
	defer client.Close()

	pctx, pcancel := context.WithTimeout(ctx, 2*cfg.Target.DialTimeout)
	err = workload.Preflight(pctx, client)
	pcancel()
	if err != nil {
		logger.Printf("preflight failed: %v", err)
		return exitPreflight
	}

	var preloadS float64
	if cfg.Workload.Preload {
		t0 := time.Now()
		if err := workload.Preload(ctx, client, cfg.Workload, logger); err != nil {
			if ctx.Err() != nil {
				logger.Printf("interrupted during preload, no report written")
				return exitOK
			}
			logger.Printf("preload failed: %v", err)
			return exitPreflight
		}
		preloadS = time.Since(t0).Seconds()
		logger.Printf("preload done in %.1fs", preloadS)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var aborted atomic.Bool
	collector := metrics.NewCollector(cfg.Metrics, cfg.Workload.Concurrency)
	collector.Start(runCtx, func() {
		aborted.Store(true)
		logger.Printf("aborting: no successful commands for %d consecutive intervals", metrics.AbortAfterEmptyIntervals)
		cancelRun()
	})

	sampleCh := sampler.Run(runCtx, client, cfg.Metrics.ServerInterval, cfg.Target.Addrs[0])
	var samples []sampler.Sample
	samplesDone := make(chan struct{})
	go func() {
		for s := range sampleCh {
			samples = append(samples, s)
		}
		close(samplesDone)
	}()

	recs := make([]workload.Recorder, cfg.Workload.Concurrency)
	for i := range recs {
		recs[i] = collector.Recorder(i)
	}
	var limiter *rate.Limiter
	if cfg.Workload.RateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.Workload.RateLimit), max(cfg.Workload.Concurrency, cfg.Workload.Pipeline))
	}
	workDone := make(chan struct{})
	go func() {
		workload.Run(runCtx, client, cfg.Workload, recs, limiter)
		close(workDone)
	}()

	exitReason := "completed"
	logger.Printf("warmup for %s", cfg.Workload.Warmup)
	if !sleep(runCtx, cfg.Workload.Warmup) {
		exitReason = reason(&aborted)
	} else {
		collector.Enable(time.Now())
		logger.Printf("measuring for %s with %d workers", cfg.Workload.Duration, cfg.Workload.Concurrency)
		if !sleep(runCtx, cfg.Workload.Duration) {
			exitReason = reason(&aborted)
		}
	}
	cancelRun()
	<-workDone
	collector.Flush(time.Now())
	<-samplesDone
	clientRes := collector.Result()
	if clientRes.Start.IsZero() {
		// Stopped during warmup: nothing was measured, so anchor the report
		// at "now" instead of the zero time.Time.
		now := time.Now()
		clientRes.Start, clientRes.End = now, now
	}

	meta := report.Meta{
		Title:       cfg.Report.Title,
		Version:     version,
		Start:       clientRes.Start,
		End:         clientRes.End,
		DurationS:   clientRes.End.Sub(clientRes.Start).Seconds(),
		PreloadS:    preloadS,
		ExitReason:  exitReason,
		GeneratedAt: time.Now(),
	}
	res := report.Build(meta, cfg, clientRes, samples)
	if err := report.Write(*outDir, res); err != nil {
		logger.Printf("report error: %v", err)
		return exitReport
	}
	logger.Printf("report written to %s: ops=%d mean=%.0f ops/s p50=%.2fms p99=%.2fms errors=%d exit=%s",
		*outDir, res.Summary.TotalOps, res.Summary.MeanOpsPerSec,
		res.Summary.P50Micros/1000, res.Summary.P99Micros/1000, res.Summary.TotalErrors, exitReason)
	if aborted.Load() {
		return exitAborted
	}
	return exitOK
}

// sleep waits d or until ctx is done; returns true if the full duration passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func reason(aborted *atomic.Bool) string {
	if aborted.Load() {
		return "aborted"
	}
	return "signal"
}
