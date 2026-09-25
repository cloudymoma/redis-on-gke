#!/usr/bin/env bash
# Redis workload lifecycle:
#   deploy <demo|sentinel|cluster>   apply the topology manifest
#   scale <n>                        set cluster-mode shard count (leaders)
#   status                           show CRs, pods, services, PVCs
#   password                         print the generated redis password
#   clean [--purge]                  delete CRs, keeping PVCs; --purge also removes PVCs and the secret
set -euo pipefail

cd "$(dirname "$0")/.."
source bin/config.sh

usage() {
    echo "Usage: $0 {deploy <demo|sentinel|cluster>|scale <n>|status|password|clean [--purge]}"
    exit 1
}

ensure_namespace() {
    kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 ||
        kubectl create namespace "${NAMESPACE}"
}

ensure_secret() {
    if ! kubectl get secret "${REDIS_SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
        echo "Generating redis password secret '${REDIS_SECRET_NAME}'"
        kubectl create secret generic "${REDIS_SECRET_NAME}" \
            --namespace "${NAMESPACE}" \
            --from-literal=password="$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 24)"
    fi
}

# Fail before applying when hard pod anti-affinity (one pod per node) cannot be
# satisfied: the surplus pods would sit Pending and the operator would stall.
require_nodes() {
    local need="$1" what="$2" have hint
    have="$(kubectl get nodes -l "cloud.google.com/gke-nodepool=${REDIS_POOL}" -o name | wc -l)"
    if [ "${have}" -lt "${need}" ]; then
        if [ "${PROFILE}" = "demo" ]; then
            hint="./bin/gke.sh scale ${need}"
        else
            hint="./bin/gke.sh scale $(((need + 2) / 3))   # nodes per zone, x3 zones"
        fi
        echo "ERROR: ${what} needs ${need} nodes in pool '${REDIS_POOL}' (one pod per node), found ${have}." >&2
        echo "Add nodes first: ${hint}" >&2
        exit 1
    fi
}

case "${1:-}" in
deploy)
    topology="${2:-}"
    template="templates/redis.${topology}.yml"
    [ -f "${template}" ] || {
        echo "Unknown topology '${topology}'." >&2
        usage
    }
    # Last clusterSize in the file: RedisCluster's shard count, or RedisSentinel's size.
    size="$(awk '/^  clusterSize:/ {n = $2} END {print n}' "${template}")"
    case "${topology}" in
    cluster) require_nodes "$((2 * size))" "cluster with ${size} shards (leader + follower each)" ;;
    sentinel) require_nodes "${size}" "${size} sentinels" ;;
    esac
    ensure_namespace
    ensure_secret
    sed -e "s/__SECRET_NAME__/${REDIS_SECRET_NAME}/g" "${template}" |
        kubectl apply --namespace "${NAMESPACE}" -f -
    echo
    echo "Deployed '${topology}'. Watch progress with:"
    echo "  kubectl get pods -n ${NAMESPACE} -w"
    ;;
scale)
    [ -n "${2:-}" ] || usage
    if ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "ERROR: shard count must be an integer, got '$2'." >&2
        exit 1
    fi
    if [ "$2" -lt 3 ]; then
        echo "ERROR: Redis Cluster requires at least 3 leaders (shards)." >&2
        exit 1
    fi
    if ! kubectl get rediscluster redis-cluster --namespace "${NAMESPACE}" >/dev/null 2>&1; then
        echo "ERROR: RedisCluster 'redis-cluster' not found in namespace '${NAMESPACE}' (only the cluster topology supports shard scaling)." >&2
        exit 1
    fi
    require_nodes "$((2 * $2))" "cluster with $2 shards (leader + follower each)"
    kubectl patch rediscluster redis-cluster \
        --namespace "${NAMESPACE}" \
        --type merge \
        --patch "{\"spec\":{\"clusterSize\":$2}}"
    echo "clusterSize set to $2. The operator will add/remove shards and reshard slots."
    # Keep the backup CronJob's shard count in sync with the new topology.
    if kubectl get cronjob redis-backup --namespace "${NAMESPACE}" >/dev/null 2>&1; then
        echo "Re-rendering backup CronJob for $2 shards"
        bin/backup.sh deploy
    fi
    ;;
status)
    kubectl get redis,redisreplication,redissentinel,rediscluster \
        --namespace "${NAMESPACE}" 2>/dev/null || true
    echo
    kubectl get pods,svc,pvc --namespace "${NAMESPACE}"
    ;;
password)
    kubectl get secret "${REDIS_SECRET_NAME}" \
        --namespace "${NAMESPACE}" \
        -o jsonpath='{.data.password}' | base64 -d
    echo
    ;;
clean)
    echo "Deleting Redis custom resources in namespace '${NAMESPACE}' (PVCs are kept: storage.keepAfterDelete)"
    kubectl delete rediscluster,redissentinel,redisreplication,redis --all \
        --namespace "${NAMESPACE}" --ignore-not-found
    if [ "${2:-}" = "--purge" ]; then
        echo "Purging PVCs and secret (data will be lost)"
        kubectl delete pvc --all --namespace "${NAMESPACE}" --ignore-not-found
        kubectl delete secret "${REDIS_SECRET_NAME}" \
            --namespace "${NAMESPACE}" --ignore-not-found
    fi
    ;;
*)
    usage
    ;;
esac
