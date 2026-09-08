package workload

import (
	"math/rand/v2"
	"strings"
	"testing"

	"redis-stress/internal/config"
)

func TestKeyGenUniformStaysInRange(t *testing.T) {
	cfg := config.Default().Workload
	cfg.KeySpace = 100
	kg := NewKeyGen(cfg, 1)
	for i := 0; i < 10_000; i++ {
		if idx := kg.Index(); idx >= 100 {
			t.Fatalf("index %d out of range", idx)
		}
	}
	if k := kg.Key(7); k != "stress:7" {
		t.Errorf("Key(7) = %q", k)
	}
	if !strings.HasPrefix(kg.Next(), "stress:") {
		t.Errorf("Next() missing prefix")
	}
}

func TestKeyGenZipfianSkewsLow(t *testing.T) {
	cfg := config.Default().Workload
	cfg.KeySpace = 10_000
	cfg.KeyDistribution = config.DistZipfian
	cfg.ZipfS = 1.2
	uni := NewKeyGen(config.Default().Workload, 1)
	zipf := NewKeyGen(cfg, 1)
	var sumU, sumZ float64
	const n = 100_000
	for i := 0; i < n; i++ {
		sumU += float64(uni.Index() % 10_000)
		sumZ += float64(zipf.Index())
	}
	if sumZ/n >= sumU/n/4 {
		t.Fatalf("zipfian mean %.0f not well below uniform mean %.0f", sumZ/n, sumU/n)
	}
}

func TestKeyGenSingleKeySpace(t *testing.T) {
	cfg := config.Default().Workload
	cfg.KeySpace = 1
	cfg.KeyDistribution = config.DistZipfian
	if idx := NewKeyGen(cfg, 3).Index(); idx != 0 {
		t.Fatalf("index = %d, want 0", idx)
	}
}

func TestPickOpMatchesRatio(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	gets := 0
	const n = 100_000
	for i := 0; i < n; i++ {
		if PickOp(rng, 0.8) == OpGet {
			gets++
		}
	}
	if ratio := float64(gets) / n; ratio < 0.78 || ratio > 0.82 {
		t.Fatalf("GET ratio = %.3f, want ~0.80", ratio)
	}
}

func TestRandomValueLength(t *testing.T) {
	v := RandomValue(rand.New(rand.NewPCG(1, 1)), 256)
	if len(v) != 256 {
		t.Fatalf("len = %d", len(v))
	}
}
