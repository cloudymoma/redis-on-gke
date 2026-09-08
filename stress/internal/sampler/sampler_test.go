package sampler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const infoFixture = "# Server\r\nredis_version:7.0.15\r\nrun_id:abcdef0123456789\r\n\r\n# Clients\r\nconnected_clients:12\r\n\r\n# Memory\r\nused_memory:1048576\r\n\r\n# Stats\r\ntotal_net_input_bytes:100\r\ntotal_net_output_bytes:200\r\ninstantaneous_ops_per_sec:4321\r\nevicted_keys:3\r\nkeyspace_hits:90\r\nkeyspace_misses:10\r\n\r\n# Replication\r\nrole:master\r\n"

func TestParseInfo(t *testing.T) {
	f := ParseInfo(infoFixture)
	want := ServerFields{Role: "master", RunID: "abcdef01", UsedMemory: 1048576, OpsPerSec: 4321,
		ConnectedClients: 12, KeyspaceHits: 90, KeyspaceMisses: 10, EvictedKeys: 3, NetInBytes: 100, NetOutBytes: 200}
	if f != want {
		t.Fatalf("ParseInfo = %+v, want %+v", f, want)
	}
}

func TestRunEmitsSamplesAndCloses(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.Set("k", "v")
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ch := Run(ctx, client, 20*time.Millisecond, srv.Addr())
	var got []Sample
	timeout := time.After(2 * time.Second)
	for len(got) < 3 {
		select {
		case s := <-ch:
			got = append(got, s)
		case <-timeout:
			t.Fatalf("only %d samples", len(got))
		}
	}
	cancel()
	for range ch { // drains until close
	}
	if got[0].Addr != srv.Addr() || got[0].NodeID != srv.Addr() {
		t.Errorf("sample identity = %+v", got[0])
	}
	if got[0].Err == "" && got[0].Fields.Keys != 1 {
		t.Errorf("Keys = %d, want 1 (DBSIZE)", got[0].Fields.Keys)
	}
}
