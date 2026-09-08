// Package config loads and validates the redis-stress YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ModeCluster    = "cluster"
	ModeStandalone = "standalone"
	ModeSentinel   = "sentinel"

	DistUniform = "uniform"
	DistZipfian = "zipfian"
)

type Config struct {
	Target   Target   `yaml:"target" json:"target"`
	Workload Workload `yaml:"workload" json:"workload"`
	Metrics  Metrics  `yaml:"metrics" json:"metrics"`
	Report   Report   `yaml:"report" json:"report"`
}

type Target struct {
	Mode           string        `yaml:"mode" json:"mode"`
	Addrs          []string      `yaml:"addrs" json:"addrs"`
	SentinelMaster string        `yaml:"sentinel_master" json:"sentinel_master"`
	PasswordEnv    string        `yaml:"password_env" json:"password_env"`
	TLS            bool          `yaml:"tls" json:"tls"`
	DialTimeout    time.Duration `yaml:"dial_timeout" json:"dial_timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout"`
}

type Workload struct {
	Duration        time.Duration `yaml:"duration" json:"duration"`
	Warmup          time.Duration `yaml:"warmup" json:"warmup"`
	Concurrency     int           `yaml:"concurrency" json:"concurrency"`
	Pipeline        int           `yaml:"pipeline" json:"pipeline"`
	ReadRatio       float64       `yaml:"read_ratio" json:"read_ratio"`
	KeySpace        int           `yaml:"key_space" json:"key_space"`
	KeyPrefix       string        `yaml:"key_prefix" json:"key_prefix"`
	KeyDistribution string        `yaml:"key_distribution" json:"key_distribution"`
	ZipfS           float64       `yaml:"zipf_s" json:"zipf_s"`
	ValueSize       int           `yaml:"value_size" json:"value_size"`
	TTL             int           `yaml:"ttl" json:"ttl"`
	Preload         bool          `yaml:"preload" json:"preload"`
	RateLimit       int           `yaml:"rate_limit" json:"rate_limit"`
}

type Metrics struct {
	Interval       time.Duration `yaml:"interval" json:"interval"`
	ServerInterval time.Duration `yaml:"server_interval" json:"server_interval"`
	Percentiles    []float64     `yaml:"percentiles" json:"percentiles"`
}

type Report struct {
	Title string `yaml:"title" json:"title"`
}

// Default returns the configuration used when a field is absent from the file.
func Default() Config {
	return Config{
		Target: Target{
			Mode:        ModeCluster,
			Addrs:       []string{"redis-cluster-leader.redis.svc:6379"},
			PasswordEnv: "REDIS_PASSWORD",
			DialTimeout: 5 * time.Second,
			ReadTimeout: 2 * time.Second,
		},
		Workload: Workload{
			Duration:        2 * time.Minute,
			Warmup:          10 * time.Second,
			Concurrency:     100,
			Pipeline:        1,
			ReadRatio:       0.8,
			KeySpace:        1_000_000,
			KeyPrefix:       "stress:",
			KeyDistribution: DistUniform,
			ZipfS:           1.1,
			ValueSize:       256,
			Preload:         true,
		},
		Metrics: Metrics{
			Interval:       time.Second,
			ServerInterval: 5 * time.Second,
			Percentiles:    []float64{50, 90, 95, 99, 99.9},
		},
	}
}

// Load reads, parses, defaults and validates the file at path.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse decodes YAML over the defaults, fills the derived title and validates.
func Parse(raw []byte) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Report.Title == "" {
		cfg.Report.Title = fmt.Sprintf("%s %s c=%d", cfg.Target.Mode, cfg.Workload.Duration, cfg.Workload.Concurrency)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate returns an error naming the first invalid field.
func (c Config) Validate() error {
	t, w, m := c.Target, c.Workload, c.Metrics
	switch t.Mode {
	case ModeCluster, ModeStandalone, ModeSentinel:
	default:
		return fmt.Errorf("target.mode: must be cluster, standalone or sentinel, got %q", t.Mode)
	}
	if len(t.Addrs) == 0 {
		return errors.New("target.addrs: at least one address is required")
	}
	if (t.Mode == ModeSentinel) != (t.SentinelMaster != "") {
		return errors.New("target.sentinel_master: required when mode is sentinel, must be empty otherwise")
	}
	if t.DialTimeout <= 0 || t.ReadTimeout <= 0 {
		return errors.New("target.dial_timeout and target.read_timeout: must be > 0")
	}
	if w.Duration <= 0 {
		return fmt.Errorf("workload.duration: must be > 0, got %s", w.Duration)
	}
	if w.Warmup < 0 {
		return fmt.Errorf("workload.warmup: must be >= 0, got %s", w.Warmup)
	}
	if w.Concurrency < 1 {
		return fmt.Errorf("workload.concurrency: must be >= 1, got %d", w.Concurrency)
	}
	if w.Pipeline < 1 {
		return fmt.Errorf("workload.pipeline: must be >= 1, got %d", w.Pipeline)
	}
	if w.ReadRatio < 0 || w.ReadRatio > 1 {
		return fmt.Errorf("workload.read_ratio: must be between 0 and 1, got %v", w.ReadRatio)
	}
	if w.KeySpace < 1 {
		return fmt.Errorf("workload.key_space: must be >= 1, got %d", w.KeySpace)
	}
	switch w.KeyDistribution {
	case DistUniform:
	case DistZipfian:
		if w.ZipfS <= 1 {
			return fmt.Errorf("workload.zipf_s: must be > 1 for zipfian, got %v", w.ZipfS)
		}
	default:
		return fmt.Errorf("workload.key_distribution: must be uniform or zipfian, got %q", w.KeyDistribution)
	}
	if w.ValueSize < 1 {
		return fmt.Errorf("workload.value_size: must be >= 1, got %d", w.ValueSize)
	}
	if w.TTL < 0 {
		return fmt.Errorf("workload.ttl: must be >= 0, got %d", w.TTL)
	}
	if w.RateLimit < 0 {
		return fmt.Errorf("workload.rate_limit: must be >= 0, got %d", w.RateLimit)
	}
	if m.Interval <= 0 {
		return fmt.Errorf("metrics.interval: must be > 0, got %s", m.Interval)
	}
	if m.ServerInterval <= 0 {
		return fmt.Errorf("metrics.server_interval: must be > 0, got %s", m.ServerInterval)
	}
	if len(m.Percentiles) == 0 {
		return errors.New("metrics.percentiles: at least one percentile is required")
	}
	for _, p := range m.Percentiles {
		if p <= 0 || p > 100 {
			return fmt.Errorf("metrics.percentiles: each value must be in (0, 100], got %v", p)
		}
	}
	return nil
}
