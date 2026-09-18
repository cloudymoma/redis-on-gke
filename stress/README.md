# redis-stress

Standalone Go load generator for the Redis deployment in this repo. One YAML
file drives it; the output is a self-contained `report.html` (charts, no
external assets) and a `report.json` twin.

## Quick start (in-cluster)

    make stress-build          # Cloud Build → Artifact Registry, writes stress/.image
    make stress-run            # cluster topology: uses stress/config.yaml
    make stress-run CONFIG=stress/config.demo.yaml   # demo topology (standalone)
    make stress-run CONFIG=stress/my-scenario.yaml

`config.yaml` targets `redis-cluster-leader` in cluster mode and will exit 3
(preflight failed) against the demo deployment; `config.demo.yaml` targets
`redis-standalone` with a load sized for the single E2 node. `build` needs the
Cloud Build and Artifact Registry APIs enabled.

`run.sh run` creates a ConfigMap from the config, launches a Job in the
`redis` namespace with the password injected from `redis-secret`, streams
logs, copies the report to `stress/reports/<timestamp>/` and deletes the Job.
The pod holds for `HOLD_SECONDS` (default 600) after writing so the copy can
happen.

## Local use (standalone/sentinel only)

Cluster mode cannot be tested through a port-forward because clients must
reach every shard directly. For the demo topology:

    kubectl port-forward -n redis svc/redis-standalone 6379:6379 &
    export REDIS_PASSWORD="$(./bin/redis.sh password)"
    sed 's|redis-standalone:6379|127.0.0.1:6379|' stress/config.demo.yaml > stress/local.yaml
    cd stress && go run ./cmd/redis-stress -config local.yaml -out ./out

Expect lower numbers than in-cluster: every command crosses the port-forward.

## Configuration

See `config.yaml`; every field has a default. Key knobs:

| Field | Meaning |
|---|---|
| `workload.concurrency` | goroutines, each with its own connection |
| `workload.pipeline` | commands per round trip; latency is then per round trip |
| `workload.read_ratio` | GET share, rest SET |
| `workload.key_space`, `key_distribution`, `zipf_s` | key range and skew |
| `workload.preload` | SET every key first so GETs hit |
| `workload.rate_limit` | cap in ops/s, 0 = unlimited |
| `metrics.server_interval` | INFO/DBSIZE sampling period per master |

## Report contents

Summary tiles, config echo, throughput over time, latency percentiles over
time, latency distribution, errors per interval and by class, per-node ops/s,
keys, memory, clients and hit ratio, node table.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | completed (or stopped by signal), report written |
| 2 | invalid config |
| 3 | preflight ping or preload failed |
| 4 | aborted: no successful commands for 10 intervals (report still written) |
| 5 | could not write the report |

## Development

    cd stress && go vet ./... && go test -race ./...
