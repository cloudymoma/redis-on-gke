#!/usr/bin/env bash
# redis-stress runner:
#   build            build the image with Cloud Build and push to Artifact Registry
#   run [config]     run the stress Job in-cluster and copy the report to stress/reports/<ts>/
#   clean            delete leftover stress Jobs and ConfigMaps
set -euo pipefail

cd "$(dirname "$0")/.."
source bin/config.sh

AR_REPO="${AR_REPO:-redis-on-gke}"
IMAGE_FILE="stress/.image"
HOLD_SECONDS="${HOLD_SECONDS:-600}"
DEADLINE_SECONDS="${DEADLINE_SECONDS:-$((3600 + HOLD_SECONDS))}"

usage() {
    echo "Usage: $0 {build|run [config.yaml]|clean}"
    exit 1
}

case "${1:-}" in
build)
    host="${REGION}-docker.pkg.dev"
    gcloud artifacts repositories describe "${AR_REPO}" \
        --project "${PROJECT_ID}" --location "${REGION}" >/dev/null 2>&1 ||
        gcloud artifacts repositories create "${AR_REPO}" \
            --project "${PROJECT_ID}" --location "${REGION}" --repository-format docker
    tag="$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)"
    [ -z "$(git status --porcelain -- stress 2>/dev/null)" ] || tag="${tag}-dirty-$(date +%H%M%S)"
    image="${host}/${PROJECT_ID}/${AR_REPO}/redis-stress:${tag}"
    gcloud builds submit stress --project "${PROJECT_ID}" --tag "${image}"
    echo "${image}" >"${IMAGE_FILE}"
    echo "Built ${image}"
    ;;
run)
    config="${2:-stress/config.yaml}"
    [ -f "${config}" ] || {
        echo "ERROR: config file '${config}' not found" >&2
        exit 1
    }
    image="${IMAGE:-$(cat "${IMAGE_FILE}" 2>/dev/null || true)}"
    [ -n "${image}" ] || {
        echo "ERROR: no image. Run '$0 build' first or export IMAGE=<ref>" >&2
        exit 1
    }
    ts="$(date +%Y%m%d-%H%M%S)"
    job="redis-stress-${ts}"
    dest="stress/reports/${ts}"
    mkdir -p "${dest}"
    cp "${config}" "${dest}/config.yaml"

    # Tear down on every exit path (a failed kubectl cp used to leak the
    # ConfigMap forever and the Job for its TTL).
    cleanup() {
        kubectl delete job "${job}" --namespace "${NAMESPACE}" --wait=false --ignore-not-found
        kubectl delete configmap "${job}" --namespace "${NAMESPACE}" --ignore-not-found
    }
    trap cleanup EXIT
    kubectl create configmap "${job}" --namespace "${NAMESPACE}" --from-file=config.yaml="${config}"
    kubectl label configmap "${job}" --namespace "${NAMESPACE}" app=redis-stress
    sed -e "s|__JOB_NAME__|${job}|g" \
        -e "s|__NAMESPACE__|${NAMESPACE}|g" \
        -e "s|__IMAGE__|${image}|g" \
        -e "s|__HOLD__|${HOLD_SECONDS}|g" \
        -e "s|__DEADLINE__|${DEADLINE_SECONDS}|g" \
        -e "s|__SECRET_NAME__|${REDIS_SECRET_NAME}|g" \
        -e "s|__CONFIGMAP__|${job}|g" \
        stress/k8s/job.yaml | kubectl apply -f -

    echo "Waiting for pod of job ${job}..."
    pod=""
    for _ in $(seq 1 60); do
        pod="$(kubectl get pod --namespace "${NAMESPACE}" -l "job-name=${job}" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
        [ -n "${pod}" ] && break
        sleep 2
    done
    [ -n "${pod}" ] || {
        echo "ERROR: pod for job ${job} did not appear" >&2
        exit 1
    }
    kubectl wait pod/"${pod}" --namespace "${NAMESPACE}" --for=condition=Ready --timeout=600s

    echo "Streaming logs until the report is ready..."
    set +o pipefail
    kubectl logs -f --namespace "${NAMESPACE}" "${pod}" | tee "${dest}/run.log" | sed '/^REPORT_READY$/q'
    set -o pipefail

    kubectl cp "${NAMESPACE}/${pod}:out/report.html" "${dest}/report.html"
    kubectl cp "${NAMESPACE}/${pod}:out/report.json" "${dest}/report.json"

    rc="$(grep -oE '^EXIT_CODE=[0-9]+' "${dest}/run.log" | tail -n 1 | cut -d= -f2 || true)"
    echo
    echo "Report: ${dest}/report.html (redis-stress exit code ${rc:-unknown})"
    [ "${rc:-1}" = "0" ] || exit 1
    ;;
clean)
    kubectl delete job --namespace "${NAMESPACE}" -l app=redis-stress --ignore-not-found
    kubectl delete configmap --namespace "${NAMESPACE}" -l app=redis-stress --ignore-not-found
    ;;
*)
    usage
    ;;
esac
