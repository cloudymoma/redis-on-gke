#!/usr/bin/env bash
# OT-Container-Kit redis-operator lifecycle: install | upgrade | status | uninstall
set -euo pipefail

cd "$(dirname "$0")/.."
source bin/config.sh

HELM_REPO_NAME="ot-helm"
HELM_REPO_URL="https://ot-container-kit.github.io/helm-charts/"
CHART="ot-helm/redis-operator"
RELEASE="redis-operator"

usage() {
    echo "Usage: $0 {install|upgrade|status|uninstall}"
    exit 1
}

case "${1:-}" in
install | upgrade)
    helm repo add "${HELM_REPO_NAME}" "${HELM_REPO_URL}" --force-update
    helm repo update "${HELM_REPO_NAME}"
    helm upgrade --install "${RELEASE}" "${CHART}" \
        --namespace "${OPERATOR_NAMESPACE}" \
        --create-namespace \
        --set featureGates.GenerateConfigInInitContainer=true \
        --wait
    ;;
status)
    helm status "${RELEASE}" --namespace "${OPERATOR_NAMESPACE}"
    kubectl get pods --namespace "${OPERATOR_NAMESPACE}"
    ;;
uninstall)
    helm uninstall "${RELEASE}" --namespace "${OPERATOR_NAMESPACE}"
    ;;
*)
    usage
    ;;
esac
