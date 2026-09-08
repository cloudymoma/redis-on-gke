#!/usr/bin/env bash
# GKE cluster lifecycle: create | scale <num-nodes-per-zone> | status | clean
set -euo pipefail

cd "$(dirname "$0")/.."
source bin/config.sh

# Minimum GKE version for the dynamic StorageClass (parameters.type: dynamic).
MIN_GKE_VERSION="1.35.3-gke.1290000"

usage() {
    echo "Usage: $0 {create [demo]|scale <num-nodes>|status|clean}"
    echo "  'create demo' is shorthand for PROFILE=demo; export PROFILE=demo for"
    echo "  scale/status/clean against the demo cluster."
    exit 1
}

case "${1:-}" in
create)
    # config.sh has already resolved the profile, so switch by re-running
    # under PROFILE=demo rather than patching individual variables here.
    if [ "${2:-}" = "demo" ] && [ "${PROFILE}" != "demo" ]; then
        exec env PROFILE=demo "$0" create
    fi

    echo "Creating GKE cluster '${CLUSTER_NAME}' in ${LOCATION} (profile: ${PROFILE}, machine: ${MACHINE_TYPE}, nodes: ${NUM_NODES}, channel: ${GKE_RELEASE_CHANNEL})..."
    gcloud container clusters create "${CLUSTER_NAME}" \
        --project "${PROJECT_ID}" \
        ${LOCATION_FLAG} \
        --release-channel "${GKE_RELEASE_CHANNEL}" \
        --num-nodes "${NUM_NODES}" \
        --machine-type "${MACHINE_TYPE}" \
        --disk-type "${DISK_TYPE}" \
        --disk-size 50 \
        --enable-ip-alias \
        --workload-pool="${PROJECT_ID}.svc.id.goog"
    gcloud container clusters get-credentials "${CLUSTER_NAME}" \
        --project "${PROJECT_ID}" ${LOCATION_FLAG}

    version="$(gcloud container clusters describe "${CLUSTER_NAME}" \
        --project "${PROJECT_ID}" ${LOCATION_FLAG} --format='value(currentMasterVersion)')"
    if [ "$(printf '%s\n' "${MIN_GKE_VERSION}" "${version}" | sort -V | head -n 1)" != "${MIN_GKE_VERSION}" ]; then
        echo "ERROR: cluster is GKE ${version}; templates/storageclass.hyperdisk.yml needs >= ${MIN_GKE_VERSION}." >&2
        echo "PVCs would stay Pending. Recreate with a newer GKE_RELEASE_CHANNEL or delete the cluster: $0 clean" >&2
        exit 1
    fi
    kubectl apply -f templates/storageclass.hyperdisk.yml
    ;;
scale)
    [ -n "${2:-}" ] || usage
    gcloud container clusters resize "${CLUSTER_NAME}" \
        --project "${PROJECT_ID}" \
        ${LOCATION_FLAG} \
        --num-nodes "$2" \
        --quiet
    ;;
status)
    gcloud container clusters describe "${CLUSTER_NAME}" \
        --project "${PROJECT_ID}" ${LOCATION_FLAG} \
        --format='table(name,status,currentNodeCount,currentMasterVersion,location)'
    ;;
clean)
    echo "Deleting GKE cluster '${CLUSTER_NAME}' in ${LOCATION} (project ${PROJECT_ID})."
    echo "This destroys all workloads and persistent disks provisioned by it."
    gcloud container clusters delete "${CLUSTER_NAME}" \
        --project "${PROJECT_ID}" ${LOCATION_FLAG}
    ;;
*)
    usage
    ;;
esac
