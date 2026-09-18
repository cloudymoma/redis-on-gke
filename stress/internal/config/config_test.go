package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseAppliesDefaultsAndOverrides(t *testing.T) {
	raw := []byte(`
target:
  mode: standalone
  addrs: ["127.0.0.1:6379"]
workload:
  duration: 30s
  concurrency: 8
  preload: false
`)
	cfg, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Target.Mode != ModeStandalone || cfg.Target.Addrs[0] != "127.0.0.1:6379" || len(cfg.Target.Addrs) != 1 {
		t.Errorf("target not overridden: %+v", cfg.Target)
	}
	if cfg.Workload.Duration != 30*time.Second || cfg.Workload.Concurrency != 8 || cfg.Workload.Preload {
		t.Errorf("workload not overridden: %+v", cfg.Workload)
	}
	if cfg.Workload.ReadRatio != 0.8 || cfg.Workload.KeySpace != 1_000_000 || cfg.Metrics.Interval != time.Second {
		t.Errorf("defaults missing: %+v %+v", cfg.Workload, cfg.Metrics)
	}
	if cfg.Report.Title != "standalone 30s c=8" {
		t.Errorf("default title = %q", cfg.Report.Title)
	}
}

func TestParseEmptyIsDefault(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil): %v", err)
	}
	if cfg.Workload.Concurrency != Default().Workload.Concurrency {
		t.Errorf("empty config should equal defaults")
	}
}

func TestParseRejectsUnknownField(t *testing.T) {
	if _, err := Parse([]byte("workload:\n  concurency: 3\n")); err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"bad mode", func(c *Config) { c.Target.Mode = "weird" }, "target.mode"},
		{"no addrs", func(c *Config) { c.Target.Addrs = nil }, "target.addrs"},
		{"sentinel needs master", func(c *Config) { c.Target.Mode = ModeSentinel }, "target.sentinel_master"},
		{"master only for sentinel", func(c *Config) { c.Target.SentinelMaster = "m" }, "target.sentinel_master"},
		{"zero duration", func(c *Config) { c.Workload.Duration = 0 }, "workload.duration"},
		{"negative warmup", func(c *Config) { c.Workload.Warmup = -time.Second }, "workload.warmup"},
		{"zero concurrency", func(c *Config) { c.Workload.Concurrency = 0 }, "workload.concurrency"},
		{"zero pipeline", func(c *Config) { c.Workload.Pipeline = 0 }, "workload.pipeline"},
		{"ratio > 1", func(c *Config) { c.Workload.ReadRatio = 1.5 }, "workload.read_ratio"},
		{"zero key space", func(c *Config) { c.Workload.KeySpace = 0 }, "workload.key_space"},
		{"bad distribution", func(c *Config) { c.Workload.KeyDistribution = "normal" }, "workload.key_distribution"},
		{"zipf s <= 1", func(c *Config) { c.Workload.KeyDistribution = DistZipfian; c.Workload.ZipfS = 1 }, "workload.zipf_s"},
		{"zero value size", func(c *Config) { c.Workload.ValueSize = 0 }, "workload.value_size"},
		{"negative ttl", func(c *Config) { c.Workload.TTL = -1 }, "workload.ttl"},
		{"negative rate", func(c *Config) { c.Workload.RateLimit = -1 }, "workload.rate_limit"},
		{"zero interval", func(c *Config) { c.Metrics.Interval = 0 }, "metrics.interval"},
		{"zero server interval", func(c *Config) { c.Metrics.ServerInterval = 0 }, "metrics.server_interval"},
		{"no percentiles", func(c *Config) { c.Metrics.Percentiles = nil }, "metrics.percentiles"},
		{"percentile > 100", func(c *Config) { c.Metrics.Percentiles = []float64{101} }, "metrics.percentiles"},
		// The report summary reads p50/p99/p99.9 by name; a list without them would
		// silently report 0.00ms in the headline tiles and the final log line.
		{"percentiles missing summary set", func(c *Config) { c.Metrics.Percentiles = []float64{90, 99} }, "metrics.percentiles"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want error mentioning %q", err, tt.want)
			}
		})
	}
	if err := Default().Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
}
