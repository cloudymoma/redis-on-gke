package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunBadConfigExits2(t *testing.T) {
	var buf bytes.Buffer
	code := run([]string{"-config", writeConfig(t, "workload:\n  concurrency: 0\n")}, &buf)
	if code != exitConfig || !strings.Contains(buf.String(), "workload.concurrency") {
		t.Fatalf("code=%d output=%q", code, buf.String())
	}
}

func TestRunUnreachableExits3(t *testing.T) {
	cfg := writeConfig(t, "target:\n  mode: standalone\n  addrs: [\"127.0.0.1:1\"]\n  password_env: \"\"\n  dial_timeout: 200ms\n")
	var buf bytes.Buffer
	if code := run([]string{"-config", cfg, "-out", t.TempDir()}, &buf); code != exitPreflight {
		t.Fatalf("code=%d output=%q", code, buf.String())
	}
}

func TestRunEndToEnd(t *testing.T) {
	srv := miniredis.RunT(t)
	cfg := writeConfig(t, `
target:
  mode: standalone
  addrs: ["`+srv.Addr()+`"]
  password_env: ""
workload:
  duration: 1500ms
  warmup: 200ms
  concurrency: 4
  pipeline: 2
  key_space: 500
  value_size: 32
metrics:
  interval: 250ms
  server_interval: 300ms
`)
	out := t.TempDir()
	var buf bytes.Buffer
	if code := run([]string{"-config", cfg, "-out", out}, &buf); code != exitOK {
		t.Fatalf("code=%d output=%s", code, buf.String())
	}
	raw, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Meta struct {
			ExitReason string `json:"exit_reason"`
		} `json:"meta"`
		Summary struct {
			TotalOps int64 `json:"total_ops"`
		} `json:"summary"`
		Client struct {
			Snapshots []any `json:"snapshots"`
		} `json:"client"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Summary.TotalOps == 0 || res.Meta.ExitReason != "completed" || len(res.Client.Snapshots) < 4 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if _, err := os.Stat(filepath.Join(out, "report.html")); err != nil {
		t.Fatal("report.html missing")
	}
	if n := len(srv.Keys()); n != 500 {
		t.Errorf("preload wrote %d keys, want 500", n)
	}
}

func TestRunSignalDuringWarmupWritesSaneReport(t *testing.T) {
	// SIGTERM before measurement starts must still yield a report whose
	// timestamps are real, not the zero time.Time.
	srv := miniredis.RunT(t)
	cfg := writeConfig(t, `
target:
  mode: standalone
  addrs: ["`+srv.Addr()+`"]
  password_env: ""
workload:
  duration: 10s
  warmup: 10s
  concurrency: 2
  key_space: 100
metrics:
  interval: 250ms
`)
	out := t.TempDir()
	go func() {
		time.Sleep(700 * time.Millisecond)
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()
	var buf bytes.Buffer
	if code := run([]string{"-config", cfg, "-out", out}, &buf); code != exitOK {
		t.Fatalf("code=%d output=%s", code, buf.String())
	}
	raw, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Meta struct {
			Start      time.Time `json:"start"`
			End        time.Time `json:"end"`
			ExitReason string    `json:"exit_reason"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Meta.ExitReason != "signal" {
		t.Errorf("exit_reason = %q, want signal", res.Meta.ExitReason)
	}
	if res.Meta.Start.IsZero() || res.Meta.End.Before(res.Meta.Start) {
		t.Errorf("meta start=%s end=%s: expected real timestamps", res.Meta.Start, res.Meta.End)
	}
}
