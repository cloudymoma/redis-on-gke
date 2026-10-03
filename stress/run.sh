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
    # Placement: the Job pins itself to the dedicated stress node. Only the
    # single-node demo cluster may run without it (sharing the Redis node).
    # Plain assignment so set -e aborts on a kubectl failure instead of
    # misreporting a missing pool.
    stress_nodes="$(kubectl get nodes -l "cloud.google.com/gke-nodepool=${STRESS_POOL}" -o name)"
    if [ -n "${stress_nodes}" ]; then
        placement_sed='/# stress-pool:/d'
    elif [ "${PROFILE}" = "demo" ]; then
        echo "NOTE: no '${STRESS_POOL}' node pool; the load generator shares the node with Redis."
        placement_sed='/# stress-pool:begin/,/# stress-pool:end/d'
    else
        echo "ERROR: no '${STRESS_POOL}' node pool. The load generator would share CPU with a Redis pod." >&2
        echo "Create it first: ./bin/gke.sh stress-pool create" >&2
        exit 1
    fi

    ts="$(date +%Y%m%d-%H%M%S)"
    job="redis-stress-${ts}"
    dest="stress/reports/${ts}"
    mkdir -p "${dest}"
    cp "${config}" "${dest}/config.yaml"

    # Tear down on every exit path (a failed kubectl cp used to leak the
    # ConfigMap forever and the Job for its TTL).
    logs_pid=""
    tail_pid=""
    cleanup() {
        # shellcheck disable=SC2086 # empty pids must vanish, not become ""
        kill ${logs_pid} ${tail_pid} 2>/dev/null || true
        kubectl delete job "${job}" --namespace "${NAMESPACE}" --wait=false --ignore-not-found
        kubectl delete configmap "${job}" --namespace "${NAMESPACE}" --ignore-not-found
    }
    trap cleanup EXIT
    kubectl create configmap "${job}" --namespace "${NAMESPACE}" --from-file=config.yaml="${config}"
    kubectl label configmap "${job}" --namespace "${NAMESPACE}" app=redis-stress
    sed -e "${placement_sed}" \
        -e "s|__STRESS_POOL__|${STRESS_POOL}|g" \
        -e "s|__JOB_NAME__|${job}|g" \
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
    # Stream to the file in the background and poll it. A `logs -f | sed q`
    # pipeline blocks until the container exits after its hold sleep, and by
    # then kubectl cp can no longer exec into the completed pod. Create the file
    # here: the background redirection opens it in the child, which can lose
    # the race with tail -f (exits on a missing file) and grep.
    : >"${dest}/run.log"
    kubectl logs -f --namespace "${NAMESPACE}" "${pod}" >"${dest}/run.log" &
    logs_pid=$!
    tail -n +1 -f "${dest}/run.log" &
    tail_pid=$!
    while ! grep -qx 'REPORT_READY' "${dest}/run.log" && kill -0 "${logs_pid}" 2>/dev/null; do
        sleep 2
    done
    sleep 1 # let tail print the last lines
    kill "${logs_pid}" "${tail_pid}" 2>/dev/null || true
    wait "${logs_pid}" "${tail_pid}" 2>/dev/null || true
    logs_pid=""
    tail_pid=""
    grep -qx 'REPORT_READY' "${dest}/run.log" || {
        echo "ERROR: log stream ended before REPORT_READY; see ${dest}/run.log" >&2
        exit 1
    }

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
