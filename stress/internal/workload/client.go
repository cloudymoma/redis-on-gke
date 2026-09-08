package workload

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"

	"redis-stress/internal/config"
)

// NewClient builds the go-redis client matching target.mode. The password is
// read from the env var named by target.password_env.
func NewClient(t config.Target, poolSize int) redis.UniversalClient {
	password := ""
	if t.PasswordEnv != "" {
		password = os.Getenv(t.PasswordEnv)
	}
	opts := &redis.UniversalOptions{
		Addrs:        t.Addrs,
		MasterName:   t.SentinelMaster,
		Password:     password,
		DialTimeout:  t.DialTimeout,
		ReadTimeout:  t.ReadTimeout,
		WriteTimeout: t.ReadTimeout,
		PoolSize:     poolSize,
		MinIdleConns: poolSize,
	}
	if t.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	switch t.Mode {
	case config.ModeCluster:
		return redis.NewClusterClient(opts.Cluster())
	case config.ModeSentinel:
		return redis.NewFailoverClient(opts.Failover())
	default:
		return redis.NewClient(opts.Simple())
	}
}

// Preflight pings every node (every shard in cluster mode).
func Preflight(ctx context.Context, client redis.UniversalClient) error {
	if cc, ok := client.(*redis.ClusterClient); ok {
		return cc.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			if err := c.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("ping %s: %w", c.Options().Addr, err)
			}
			return nil
		})
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}
