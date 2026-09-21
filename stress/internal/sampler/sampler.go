// Package sampler polls INFO and DBSIZE on each Redis master during a run.
package sampler

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ServerFields is the subset of INFO the report uses, plus DBSIZE.
type ServerFields struct {
	Role             string `json:"role"`
	RunID            string `json:"run_id"`
	UsedMemory       int64  `json:"used_memory"`
	OpsPerSec        int64  `json:"ops_per_sec"`
	ConnectedClients int64  `json:"connected_clients"`
	KeyspaceHits     int64  `json:"keyspace_hits"`
	KeyspaceMisses   int64  `json:"keyspace_misses"`
	EvictedKeys      int64  `json:"evicted_keys"`
	NetInBytes       int64  `json:"net_in_bytes"`
	NetOutBytes      int64  `json:"net_out_bytes"`
	Keys             int64  `json:"keys"`
}

// Sample is one observation of one node. NodeID is the node address.
type Sample struct {
	At     time.Time    `json:"at"`
	NodeID string       `json:"node_id"`
	Addr   string       `json:"addr"`
	Fields ServerFields `json:"fields"`
	Err    string       `json:"err,omitempty"`
}

// ParseInfo extracts ServerFields from INFO output.
func ParseInfo(info string) ServerFields {
	var f ServerFields
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch k {
		case "run_id":
			f.RunID = v
			if len(v) > 8 {
				f.RunID = v[:8]
			}
		case "role":
			f.Role = v
		case "used_memory":
			f.UsedMemory = atoi(v)
		case "instantaneous_ops_per_sec":
			f.OpsPerSec = atoi(v)
		case "connected_clients":
			f.ConnectedClients = atoi(v)
		case "keyspace_hits":
			f.KeyspaceHits = atoi(v)
		case "keyspace_misses":
			f.KeyspaceMisses = atoi(v)
		case "evicted_keys":
			f.EvictedKeys = atoi(v)
		case "total_net_input_bytes":
			f.NetInBytes = atoi(v)
		case "total_net_output_bytes":
			f.NetOutBytes = atoi(v)
		}
	}
	return f
}

func atoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// Run samples immediately and then every interval until ctx is done. In
// cluster mode every master is sampled; otherwise the client itself, labelled
// with fallbackAddr. The channel is closed when the goroutine exits.
func Run(ctx context.Context, client redis.UniversalClient, interval time.Duration, fallbackAddr string) <-chan Sample {
	ch := make(chan Sample, 64)
	go func() {
		defer close(ch)
		t := time.NewTicker(interval)
		defer t.Stop()
		collect := func() {
			if cc, ok := client.(*redis.ClusterClient); ok {
				_ = cc.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
					send(ctx, ch, sample(ctx, c, c.Options().Addr))
					return nil
				})
				return
			}
			send(ctx, ch, sample(ctx, client, fallbackAddr))
		}
		collect()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				collect()
			}
		}
	}()
	return ch
}

func send(ctx context.Context, ch chan<- Sample, s Sample) {
	select {
	case ch <- s:
	case <-ctx.Done():
	}
}

func sample(ctx context.Context, c redis.Cmdable, addr string) Sample {
	s := Sample{At: time.Now(), NodeID: addr, Addr: addr}
	info, err := c.Info(ctx).Result()
	if err != nil {
		s.Err = err.Error()
		return s
	}
	s.Fields = ParseInfo(info)
	n, err := c.DBSize(ctx).Result()
	if err != nil {
		s.Err = err.Error()
		return s
	}
	s.Fields.Keys = n
	return s
}
