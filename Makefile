# Redis on GKE — modeled after elasticsearch-cn/elastic-on-gke.
# Default topology is cluster mode (sharded). See README.md.

.PHONY: all gke operator cluster sentinel demo scale status password \
        backup-setup backup-deploy backup-run clean-redis clean \
        demo-all demo-gke

# One-shot production: GKE cluster (regional C4) + operator + Redis Cluster
all: gke operator cluster

gke:
	./bin/gke.sh create

# One-shot demo: GKE cluster (single-zone E2) + operator + standalone Redis
demo-all: demo-gke operator demo

demo-gke:
	./bin/gke.sh create demo

operator:
	./bin/operator.sh install

cluster: operator
	./bin/redis.sh deploy cluster

sentinel: operator
	./bin/redis.sh deploy sentinel

demo: operator
	./bin/redis.sh deploy demo

# usage: make scale N=5
scale:
	./bin/redis.sh scale $(N)

status:
	./bin/redis.sh status

password:
	./bin/redis.sh password

backup-setup:
	./bin/backup.sh setup

backup-deploy:
	./bin/backup.sh deploy

backup-run:
	./bin/backup.sh run

clean-redis:
	./bin/redis.sh clean

clean:
	./bin/gke.sh clean

# --- stress test (see stress/README.md) ---
.PHONY: stress-build stress-run
stress-build:
	./stress/run.sh build

# usage: make stress-run [CONFIG=stress/my.yaml]
stress-run:
	./stress/run.sh run $(CONFIG)
