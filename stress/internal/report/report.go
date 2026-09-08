// Package report turns a run's data into report.json and a self-contained
// report.html with embedded Chart.js.
package report

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"time"

	"redis-stress/internal/config"
	"redis-stress/internal/metrics"
	"redis-stress/internal/sampler"
)

//go:embed assets/chart.umd.js
var chartJS string

//go:embed assets/report.html.tmpl
var tmplSrc string

var tmpl = template.Must(template.New("report").Parse(tmplSrc))

type Meta struct {
	Title       string    `json:"title"`
	Version     string    `json:"version"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	DurationS   float64   `json:"duration_s"`
	PreloadS    float64   `json:"preload_s"`
	ExitReason  string    `json:"exit_reason"`
	GeneratedAt time.Time `json:"generated_at"`
}

// Summary holds the headline numbers. Latencies are microseconds.
// HitRatio is -1 when the server did not report keyspace stats.
type Summary struct {
	TotalOps      int64   `json:"total_ops"`
	MeanOpsPerSec float64 `json:"mean_ops_per_sec"`
	P50Micros     float64 `json:"p50_us"`
	P99Micros     float64 `json:"p99_us"`
	P999Micros    float64 `json:"p999_us"`
	MaxMicros     float64 `json:"max_us"`
	TotalErrors   int64   `json:"total_errors"`
	ErrorRate     float64 `json:"error_rate"`
	HitRatio      float64 `json:"hit_ratio"`
}

type NodeSeries struct {
	NodeID  string           `json:"node_id"`
	Addr    string           `json:"addr"`
	Role    string           `json:"role"`
	Samples []sampler.Sample `json:"samples"`
}

type Result struct {
	Meta    Meta                 `json:"meta"`
	Config  config.Config        `json:"config"`
	Summary Summary              `json:"summary"`
	Client  metrics.ClientResult `json:"client"`
	Server  []NodeSeries         `json:"server"`
}

// Build assembles the Result and derives the Summary. Samples taken before
// meta.Start (warmup) are dropped.
func Build(meta Meta, cfg config.Config, client metrics.ClientResult, samples []sampler.Sample) Result {
	r := Result{Meta: meta, Config: cfg, Client: client}

	var ops int64
	for _, n := range client.TotalOps {
		ops += n
	}
	var errs int64
	for _, n := range client.TotalErrors {
		errs += n
	}
	s := Summary{
		TotalOps:    ops,
		TotalErrors: errs,
		P50Micros:   client.OverallAll.Values["p50"],
		P99Micros:   client.OverallAll.Values["p99"],
		P999Micros:  client.OverallAll.Values["p99.9"],
		MaxMicros:   client.OverallAll.Max,
		HitRatio:    -1,
	}
	if meta.DurationS > 0 {
		s.MeanOpsPerSec = float64(ops) / meta.DurationS
	}
	if ops+errs > 0 {
		s.ErrorRate = float64(errs) / float64(ops+errs)
	}

	index := map[string]int{}
	for _, smp := range samples {
		if smp.At.Before(meta.Start) {
			continue
		}
		i, ok := index[smp.NodeID]
		if !ok {
			i = len(r.Server)
			index[smp.NodeID] = i
			r.Server = append(r.Server, NodeSeries{NodeID: smp.NodeID, Addr: smp.Addr})
		}
		r.Server[i].Samples = append(r.Server[i].Samples, smp)
		if smp.Fields.Role != "" {
			r.Server[i].Role = smp.Fields.Role
		}
	}

	// Keyspace deltas use the first and last *successful* sample per node;
	// a failed sample carries zero-valued Fields.
	var hits, misses int64
	for _, n := range r.Server {
		var first, last *sampler.ServerFields
		for i := range n.Samples {
			if n.Samples[i].Err != "" {
				continue
			}
			if first == nil {
				first = &n.Samples[i].Fields
			}
			last = &n.Samples[i].Fields
		}
		if first == nil || first == last {
			continue
		}
		hits += last.KeyspaceHits - first.KeyspaceHits
		misses += last.KeyspaceMisses - first.KeyspaceMisses
	}
	if hits+misses > 0 {
		s.HitRatio = float64(hits) / float64(hits+misses)
	}
	r.Summary = s
	return r
}

func marshal(r Result) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(true)
	enc.SetIndent("", " ")
	if err := enc.Encode(r); err != nil {
		return nil, fmt.Errorf("encode result: %w", err)
	}
	return buf.Bytes(), nil
}

// Render produces the HTML report.
func Render(r Result) ([]byte, error) {
	data, err := marshal(r)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]any{
		"Title":   r.Meta.Title,
		"Data":    template.JS(data),
		"ChartJS": template.JS(chartJS),
	})
	if err != nil {
		return nil, fmt.Errorf("render template: %w", err)
	}
	return buf.Bytes(), nil
}

// Write stores report.json first (cheapest artifact), then report.html.
func Write(dir string, r Result) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	data, err := marshal(r)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), data, 0o644); err != nil {
		return fmt.Errorf("write report.json: %w", err)
	}
	html, err := Render(r)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.html"), html, 0o644); err != nil {
		return fmt.Errorf("write report.html: %w", err)
	}
	return nil
}
