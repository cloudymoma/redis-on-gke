package workload

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"redis-stress/internal/config"
)

// Logger is the subset of *log.Logger the workload needs.
type Logger interface {
	Printf(format string, args ...any)
}

const preloadBatch = 1000

// Preload SETs every key in [0, key_space) in pipelined batches so that GETs
// during the run hit existing keys. Logs progress every 10%.
func Preload(ctx context.Context, client redis.UniversalClient, cfg config.Workload, log Logger) error {
	value := RandomValue(rand.New(rand.NewPCG(0, 0)), cfg.ValueSize)
	kg := NewKeyGen(cfg, 0)
	total := cfg.KeySpace
	for start := 0; start < total; start += preloadBatch {
		end := min(start+preloadBatch, total)
		pipe := client.Pipeline()
		for i := start; i < end; i++ {
			pipe.Set(ctx, kg.Key(uint64(i)), value, 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("preload keys %d-%d: %w", start, end-1, err)
		}
		if (end*10)/total > (start*10)/total {
			log.Printf("preload: %d/%d keys (%d%%)", end, total, end*100/total)
		}
	}
	return nil
}

// Run starts cfg.Concurrency workers and blocks until ctx is done and every
// worker has exited. recs[i] receives worker i's observations.
func Run(ctx context.Context, client redis.UniversalClient, cfg config.Workload, recs []Recorder, limiter *rate.Limiter) {
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker(ctx, client, cfg, recs[i], limiter, uint64(i+1))
		}(i)
	}
	wg.Wait()
}

func worker(ctx context.Context, client redis.UniversalClient, cfg config.Workload, rec Recorder, limiter *rate.Limiter, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed*7919))
	kg := NewKeyGen(cfg, seed)
	value := RandomValue(rng, cfg.ValueSize)
	ttl := time.Duration(cfg.TTL) * time.Second
	ops := make([]Op, cfg.Pipeline)
	keys := make([]string, cfg.Pipeline)

	for ctx.Err() == nil {
		if limiter != nil {
			if err := limiter.WaitN(ctx, cfg.Pipeline); err != nil {
				return
			}
		}
		for j := range ops {
			ops[j] = PickOp(rng, cfg.ReadRatio)
			keys[j] = kg.Next()
		}
		if cfg.Pipeline == 1 {
			start := time.Now()
			err := execOne(ctx, client, ops[0], keys[0], value, ttl)
			if ctx.Err() != nil {
				return
			}
			rec.Record(ops[0], time.Since(start), err)
			continue
		}
		pipe := client.Pipeline()
		for j := range ops {
			queue(ctx, pipe, ops[j], keys[j], value, ttl)
		}
		start := time.Now()
		cmds, _ := pipe.Exec(ctx)
		lat := time.Since(start)
		if ctx.Err() != nil {
			return
		}
		for j, cmd := range cmds {
			rec.Record(ops[j], lat, normalize(cmd.Err()))
		}
	}
}

func execOne(ctx context.Context, c redis.Cmdable, op Op, key string, value []byte, ttl time.Duration) error {
	if op == OpGet {
		return normalize(c.Get(ctx, key).Err())
	}
	return c.Set(ctx, key, value, ttl).Err()
}

func queue(ctx context.Context, pipe redis.Pipeliner, op Op, key string, value []byte, ttl time.Duration) {
	if op == OpGet {
		pipe.Get(ctx, key)
		return
	}
	pipe.Set(ctx, key, value, ttl)
}

// normalize treats a GET miss as success.
func normalize(err error) error {
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}
