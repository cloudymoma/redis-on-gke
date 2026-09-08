# Redis on GKE

Deploy, manage, and scale **Redis Cluster** on Google Kubernetes Engine using the
[OT-Container-Kit redis-operator](https://github.com/OT-CONTAINER-KIT/redis-operator)
(Apache-2.0). Structure and workflow are modeled after
[elasticsearch-cn/elastic-on-gke](https://github.com/elasticsearch-cn/elastic-on-gke),
with the operator playing the role ECK plays there: you declare the desired
topology in YAML, and the operator handles pod orchestration, slot resharding,
and failover.

## Layout

```
├── Makefile               # topology targets (cluster is the default)
├── bin/
│   ├── config.sh          # shared config — set PROJECT_ID/REGION here or via env
│   ├── gke.sh             # GKE cluster: create | scale <n> | status | clean
│   ├── operator.sh        # redis-operator: install | upgrade | status | uninstall
│   ├── redis.sh           # workloads: deploy | scale | status | password | clean
│   └── backup.sh          # GCS backups: setup | deploy | run | status
└── templates/
    ├── redis.cluster.yml  # PRIMARY: cluster mode, 3 shards + 3 replicas
    ├── redis.sentinel.yml # HA without sharding (replication + sentinel)
    ├── redis.demo.yml     # single standalone node (dev/PoC)
    ├── backup.cronjob.yml # nightly per-shard RDB → GCS
    └── storageclass.hyperdisk.yml # dynamic class: Hyperdisk on C4, pd-balanced on E2
```

## Prerequisites

- `gcloud` (authenticated, with a default project or `PROJECT_ID` exported),
  `kubectl`, `helm` 3+
- APIs enabled in the project (Cloud Build and Artifact Registry are only
  needed for the stress test image):

  ```bash
  gcloud services enable container.googleapis.com \
      cloudbuild.googleapis.com artifactregistry.googleapis.com
  ```
- GKE **1.35.3-gke.1290000 or later** (the dynamic StorageClass needs it).
  `bin/gke.sh create` uses the `rapid` release channel by default
  (`GKE_RELEASE_CHANNEL` to override) and fails if the created cluster is older.

## Quickstart (cluster mode)

```bash
export PROJECT_ID=my-project   # or rely on gcloud default
export REGION=us-central1      # optional, this is the default

make all        # = make gke + make operator + make cluster
make status
make password   # auth password for the 'elastic'-equivalent: user 'default'
```

`make all` does three things:

1. **`gke`** — creates a regional GKE cluster (3 zones in `us-central1`,
   `c4-standard-4`, Workload Identity enabled) via `bin/gke.sh create`.
2. **`operator`** — installs the redis-operator Helm chart into `ot-operators`.
3. **`cluster`** — creates the `redis` namespace, generates a password Secret,
   and applies `templates/redis.cluster.yml`: **3 leaders (shards) + 3
   followers**, persistence on `hyperdisk-balanced` PVCs, redis-exporter
   sidecars, PodDisruptionBudgets, and soft anti-affinity spreading pods
   across zones and nodes.

Connect from inside the cluster (cluster-aware client required):

```bash
kubectl run redis-cli --rm -it --restart=Never -n redis \
  --image=redis:7.2-alpine -- \
  redis-cli -c -h redis-cluster-leader -a "$(./bin/redis.sh password)"
```

Application clients should use `redis-cluster-leader.redis.svc:6379` as the
seed address with cluster mode enabled (e.g. `redis.NewClusterClient` in
go-redis, `RedisCluster` in redis-py).

## Demo Mode (Low-Cost Single-Zone)

For functional testing, dev, or local verification without the cost of a
regional C4 cluster. The demo is one zonal `e2-standard-4` node in
`us-central1-a` with `pd-balanced` disks, the operator, and a single
standalone Redis pod (no HA, no sharding).

Every command that targets the demo cluster needs `PROFILE=demo`; without it
the scripts address the prod cluster `redis-gke`. Export it once:

```bash
export PROJECT_ID=my-project   # or rely on gcloud default
export PROFILE=demo
```

1. **Create everything** (about 10 minutes, most of it cluster creation):

   ```bash
   make demo-all   # = ./bin/gke.sh create demo + operator install + deploy demo
   ```

   `gke.sh create` also configures `kubectl` for the new cluster and applies
   the dynamic StorageClass, so `hyperdisk-balanced` PVCs land on `pd-balanced`
   on the E2 node.

2. **Wait for Redis** to be ready, then read the password:

   ```bash
   kubectl get pods -n redis -w      # until redis-standalone-0 is 2/2 Running
   make status
   make password
   ```

3. **Connect** (standalone, so no `-c` cluster flag):

   ```bash
   kubectl run redis-cli --rm -it --restart=Never -n redis \
     --image=redis:7.2-alpine -- \
     redis-cli -h redis-standalone -a "$(./bin/redis.sh password)" ping
   ```

   In-cluster address for applications: `redis-standalone.redis.svc:6379`.

4. **Stress test it** with the demo-sized config (see `stress/README.md`):

   ```bash
   make stress-build
   make stress-run CONFIG=stress/config.demo.yaml
   # report lands in stress/reports/<timestamp>/report.html
   ```

5. **Tear down.** Delete the Redis resources and PVCs first so the disks are
   released, then the cluster:

   ```bash
   ./bin/redis.sh clean --purge
   make clean                       # PROFILE=demo is still exported
   ```

## Scaling

### Shards (horizontal, the cluster-mode superpower)

```bash
make scale N=5        # or: ./bin/redis.sh scale 5
```

This patches `spec.clusterSize`; the operator adds leader/follower pairs and
**reshards the 16384 hash slots** onto new shards. Scale-in migrates slots off
removed shards first. If the backup CronJob is deployed, `scale` re-renders
it automatically so new shards are included in the next backup. Never scale the underlying StatefulSets directly —
that's the equivalent of editing `nodeSets.count` by hand behind ECK's back.
Minimum is 3 shards (quorum floor).

### GKE nodes

```bash
./bin/gke.sh scale 2   # nodes per zone (regional cluster ⇒ ×3 total)
```

Keep the cluster autoscaler off the Redis node pool, or rely on the PDBs
(`maxUnavailable: 1`) in the manifest to stop evictions taking out a leader
and its follower together.

### Vertical

Edit resources/storage in `templates/redis.cluster.yml` and re-apply with
`./bin/redis.sh deploy cluster`. The operator performs a rolling restart,
replicas first. Note: PVC size can only grow; `storageclass.hyperdisk.yml`
sets `allowVolumeExpansion: true`.

## Backups (RDB → GCS)

```bash
make backup-setup    # bucket + service account + Workload Identity binding
make backup-deploy   # nightly CronJob (03:00), one RDB per shard
make backup-run      # trigger an immediate backup
./bin/backup.sh status
```

The CronJob streams an RDB from each leader with `redis-cli --rdb` (no PVC
access needed) and uploads to `gs://$PROJECT_ID-redis-backups/<timestamp>/`.
This is the analog of elastic-on-gke's GCS snapshot repository, minus the
in-product snapshot API Redis OSS doesn't have.

## Exposure and security

- **In-cluster access only by default.** Unlike the Elasticsearch setup, do
  not put Redis behind a public load balancer: cluster-mode clients must
  reach every shard directly (they follow `MOVED` redirects), and Redis is
  designed for trusted networks. If external access is unavoidable, use an
  internal (VPC) load balancer per shard or a VPN/bastion.
- Auth is enabled: a random password is generated into the `redis-secret`
  Secret on first deploy (`./bin/redis.sh password` to read it).
- TLS is not enabled in these templates; the operator supports it via
  `spec.TLS` with a cert-manager–issued secret if you need it.

## Other topologies

| Target | Manifest | What you get | When |
|---|---|---|---|
| `make cluster` | `redis.cluster.yml` | 3 shards + 3 replicas, sharded | Dataset > one node's RAM, horizontal scale (default) |
| `make sentinel` | `redis.sentinel.yml` | 1 master + 2 replicas + 3 sentinels | HA only, simpler clients, multi-key ops unrestricted |
| `make demo` | `redis.demo.yml` | 1 standalone pod | Dev/PoC |

## Upgrades

- **Redis version**: bump the image tag in the template and re-apply — the
  operator rolls pods with failover, same workflow as editing `spec.version`
  in ECK. Don't downgrade.
- **Operator**: `./bin/operator.sh upgrade` (may trigger rolling restarts).

## Teardown

```bash
./bin/redis.sh clean          # delete Redis CRs, keep data (PVCs) and secret
./bin/redis.sh clean --purge  # also delete PVCs and secret — DATA LOSS
make clean                    # delete the whole GKE cluster
```

Run `clean --purge` before `make clean`: deleting a cluster does not delete
disks behind still-existing PVCs, and they keep billing as orphans.

## Mapping to elastic-on-gke

| elastic-on-gke | redis-on-gke |
|---|---|
| ECK operator | OT-Container-Kit redis-operator |
| `Elasticsearch` CR, `spec.nodeSets.count` | `RedisCluster` CR, `spec.clusterSize` |
| `bin/gke.sh` create/scale | same |
| `bin/es.sh` deploy/password | `bin/redis.sh` deploy/password |
| `bin/glb.sh` (public HTTPS GLB) | intentionally absent — keep Redis private |
| GCS snapshot repository | `bin/backup.sh` + RDB CronJob |
| `make init_single/allrole/prod` | `make demo/sentinel/cluster` |
