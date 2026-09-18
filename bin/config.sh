# Shared configuration for all scripts. Override any value via environment
# variables, e.g. REGION=europe-west1 ./bin/gke.sh create

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"

ZONE="${ZONE:-${REGION}-a}"

# Deployment profile (export PROFILE=demo for every command that targets the
# demo cluster: create, status, scale, clean):
#   prod (default) : regional (3 zones x 2 nodes), c4-standard-4, hyperdisk-balanced (HA)
#   demo           : single-zone, e2-standard-4, pd-balanced (low cost, functional testing)
PROFILE="${PROFILE:-prod}"

case "${PROFILE}" in
prod | demo) ;;
*)
    echo "ERROR: PROFILE must be 'prod' or 'demo', got '${PROFILE}'." >&2
    exit 1
    ;;
esac

if [ "${PROFILE}" = "demo" ]; then
    CLUSTER_NAME="${CLUSTER_NAME:-redis-demo}"
    LOCATION="${ZONE}"
    LOCATION_FLAG="--zone=${ZONE}"
    MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-4}"
    DISK_TYPE="${DISK_TYPE:-pd-balanced}"
    NUM_NODES="${NUM_NODES:-1}" # single zone = 1 node total
else
    CLUSTER_NAME="${CLUSTER_NAME:-redis-gke}"
    LOCATION="${REGION}"
    LOCATION_FLAG="--region=${REGION}"
    MACHINE_TYPE="${MACHINE_TYPE:-c4-standard-4}"
    DISK_TYPE="${DISK_TYPE:-hyperdisk-balanced}"
    NUM_NODES="${NUM_NODES:-2}" # per zone (regional = 3 zones, 6 nodes total): one node per cluster pod, see redis.cluster.yml anti-affinity
fi

# GKE release channel. The dynamic StorageClass (templates/storageclass.hyperdisk.yml)
# needs GKE >= 1.35.3-gke.1290000; bin/gke.sh create verifies this after creation.
GKE_RELEASE_CHANNEL="${GKE_RELEASE_CHANNEL:-rapid}"

# Kubernetes
NAMESPACE="${NAMESPACE:-redis}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-ot-operators}"
REDIS_SECRET_NAME="${REDIS_SECRET_NAME:-redis-secret}"

# Backups
GCS_BUCKET="${GCS_BUCKET:-${PROJECT_ID}-redis-backups}"
BACKUP_GSA_NAME="${BACKUP_GSA_NAME:-redis-backup}"
BACKUP_KSA_NAME="${BACKUP_KSA_NAME:-redis-backup}"

if [ -z "${PROJECT_ID}" ] || [ "${PROJECT_ID}" = "(unset)" ]; then
    echo "ERROR: PROJECT_ID is not set and no default gcloud project configured." >&2
    echo "Run: gcloud config set project <your-project>  (or export PROJECT_ID)" >&2
    exit 1
fi
