#!/usr/bin/env bash
# GCS backups for the cluster topology via Workload Identity:
#   setup    create GCS bucket + Google service account + Workload Identity binding
#   deploy   render and apply the backup CronJob
#   run      trigger an immediate backup job from the CronJob
#   status   show recent backup jobs and bucket contents
set -euo pipefail

cd "$(dirname "$0")/.."
source bin/config.sh

GSA_EMAIL="${BACKUP_GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

usage() {
    echo "Usage: $0 {setup|deploy|run|status}"
    exit 1
}

case "${1:-}" in
setup)
    gcloud storage buckets describe "gs://${GCS_BUCKET}" >/dev/null 2>&1 ||
        gcloud storage buckets create "gs://${GCS_BUCKET}" \
            --project "${PROJECT_ID}" --location "${REGION}"

    gcloud iam service-accounts describe "${GSA_EMAIL}" >/dev/null 2>&1 ||
        gcloud iam service-accounts create "${BACKUP_GSA_NAME}" \
            --project "${PROJECT_ID}" \
            --display-name "Redis GCS backups"

    gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
        --member "serviceAccount:${GSA_EMAIL}" \
        --role roles/storage.objectAdmin

    kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 ||
        kubectl create namespace "${NAMESPACE}"
    kubectl get serviceaccount "${BACKUP_KSA_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1 ||
        kubectl create serviceaccount "${BACKUP_KSA_NAME}" -n "${NAMESPACE}"

    gcloud iam service-accounts add-iam-policy-binding "${GSA_EMAIL}" \
        --project "${PROJECT_ID}" \
        --role roles/iam.workloadIdentityUser \
        --member "serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${BACKUP_KSA_NAME}]"
    kubectl annotate serviceaccount "${BACKUP_KSA_NAME}" -n "${NAMESPACE}" \
        --overwrite "iam.gke.io/gcp-service-account=${GSA_EMAIL}"
    echo "Backup prerequisites ready: gs://${GCS_BUCKET} writable by ${NAMESPACE}/${BACKUP_KSA_NAME}"
    ;;
deploy)
    if ! cluster_size="$(kubectl get rediscluster redis-cluster -n "${NAMESPACE}" \
        -o jsonpath='{.spec.clusterSize}')"; then
        echo "ERROR: RedisCluster 'redis-cluster' not found in namespace '${NAMESPACE}'." >&2
        echo "Deploy it first: ./bin/redis.sh deploy cluster" >&2
        exit 1
    fi
    sed -e "s/__GCS_BUCKET__/${GCS_BUCKET}/g" \
        -e "s/__NAMESPACE__/${NAMESPACE}/g" \
        -e "s/__CLUSTER_SIZE__/${cluster_size}/g" \
        -e "s/__KSA_NAME__/${BACKUP_KSA_NAME}/g" \
        -e "s/__SECRET_NAME__/${REDIS_SECRET_NAME}/g" \
        templates/backup.cronjob.yml | kubectl apply -f -
    ;;
run)
    kubectl create job --namespace "${NAMESPACE}" \
        --from=cronjob/redis-backup "redis-backup-manual-$(date +%s)"
    ;;
status)
    kubectl get cronjob,job --namespace "${NAMESPACE}"
    echo
    if ! gcloud storage ls -r "gs://${GCS_BUCKET}" | tail -n 20; then
        echo "ERROR: cannot list gs://${GCS_BUCKET}. If it does not exist yet, run: ./bin/backup.sh setup" >&2
        exit 1
    fi
    ;;
*)
    usage
    ;;
esac
