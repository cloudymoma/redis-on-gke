// Package workload generates load against Redis: key/value generators,
// the client factory and the worker loop.
package workload

import (
	"math/rand/v2"
	"strconv"
	"time"

	"redis-stress/internal/config"
)

// Op is a Redis command type exercised by the workload.
type Op string

const (
	OpGet Op = "GET"
	OpSet Op = "SET"
)

// Recorder receives one observation per executed command.
type Recorder interface {
	Record(op Op, latency time.Duration, err error)
}

// KeyGen draws key indexes from the configured distribution.
type KeyGen struct {
	prefix string
	space  uint64
	rng    *rand.Rand
	zipf   *rand.Zipf
}

func NewKeyGen(cfg config.Workload, seed uint64) *KeyGen {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	k := &KeyGen{prefix: cfg.KeyPrefix, space: uint64(cfg.KeySpace), rng: rng}
	if cfg.KeyDistribution == config.DistZipfian {
		k.zipf = rand.NewZipf(rng, cfg.ZipfS, 1, k.space-1)
	}
	return k
}

func (k *KeyGen) Index() uint64 {
	if k.zipf != nil {
		return k.zipf.Uint64()
	}
	return k.rng.Uint64N(k.space)
}

func (k *KeyGen) Key(i uint64) string { return k.prefix + strconv.FormatUint(i, 10) }

func (k *KeyGen) Next() string { return k.Key(k.Index()) }

// RandomValue returns n random lowercase letters. Letters rather than raw
// bytes keep the payload printable when inspecting keys with redis-cli.
func RandomValue(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(rng.IntN(26))
	}
	return b
}

// PickOp returns OpGet with probability readRatio, otherwise OpSet.
func PickOp(rng *rand.Rand, readRatio float64) Op {
	if rng.Float64() < readRatio {
		return OpGet
	}
	return OpSet
}
