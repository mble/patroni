#!/usr/bin/env bash
set -euo pipefail

readonly CLUSTER_NAME="${K3D_CLUSTER_NAME:-controller-agent}"
readonly IMAGE_MODE="${K3D_IMAGE_MODE:-import}"
readonly NAMESPACE=database
readonly STATEFULSET=patroni-split
readonly POD_COUNT=3
readonly TARGET_POD=patroni-split-0
readonly CONTROL_PATH=/run/patroni/agent.sock
readonly ACCEPTANCE_PASSWORD=acceptance-superuser
readonly WAIT_ATTEMPTS=90
readonly WAIT_SECONDS=2
readonly FENCE_WAIT_ATTEMPTS=30
readonly EVIDENCE_DIR="${1:-role-image-artifacts/kubernetes}"
paused_pod=''

fail() {
    echo "controller-agent Kubernetes check: $*" >&2
    exit 1
}

restart_count() {
    local container="$1"

    kubectl --namespace "${NAMESPACE}" get pod "${TARGET_POD}" \
        --output "jsonpath={.status.containerStatuses[?(@.name==\"${container}\")].restartCount}"
}

wait_restart() {
    local container="$1"
    local before="$2"
    local current
    local ready

    for ((attempt = 0; attempt < WAIT_ATTEMPTS; attempt++)); do
        current="$(restart_count "${container}")"
        ready="$(kubectl --namespace "${NAMESPACE}" get pod "${TARGET_POD}" \
            --output 'jsonpath={.status.conditions[?(@.type=="Ready")].status}')"
        if ((current > before)) && [[ "${ready}" == True ]]; then
            return
        fi
        sleep "${WAIT_SECONDS}"
    done

    fail "${container} did not restart"
}

signal_controller() {
    local pod="$1"
    local signal_name="$2"

    kubectl --namespace "${NAMESPACE}" exec "${pod}" --container controller -- \
        /usr/bin/python3 -c '
import os
import pathlib
import signal
import sys

current = os.getpid()
for entry in pathlib.Path("/proc").iterdir():
    if not entry.name.isdigit():
        continue
    pid = int(entry.name)
    if pid in (1, current):
        continue
    try:
        command = (entry / "cmdline").read_bytes()
    except OSError:
        continue
    if b"\0patroni.controller\0" not in command:
        continue
    os.kill(pid, getattr(signal, "SIG" + sys.argv[1]))
    raise SystemExit(0)
raise SystemExit("controller child not found")
' "${signal_name}"
}

collect() {
    mkdir -p "${EVIDENCE_DIR}"
    kubectl --namespace "${NAMESPACE}" get all,pvc --output wide \
        >"${EVIDENCE_DIR}/resources.txt" 2>&1 || true
    kubectl --namespace "${NAMESPACE}" get events --sort-by=.lastTimestamp \
        >"${EVIDENCE_DIR}/events.txt" 2>&1 || true
    kubectl --namespace "${NAMESPACE}" get statefulset "${STATEFULSET}" --output yaml \
        >"${EVIDENCE_DIR}/statefulset.yaml" 2>&1 || true
    for ordinal in $(seq 0 $((POD_COUNT - 1))); do
        for container in agent controller; do
            kubectl --namespace "${NAMESPACE}" logs "${STATEFULSET}-${ordinal}" \
                --container "${container}" >"${EVIDENCE_DIR}/${STATEFULSET}-${ordinal}-${container}.log" 2>&1 || true
            kubectl --namespace "${NAMESPACE}" logs "${STATEFULSET}-${ordinal}" \
                --container "${container}" --previous \
                >"${EVIDENCE_DIR}/${STATEFULSET}-${ordinal}-${container}-previous.log" 2>&1 || true
        done
    done
}

finish() {
    if [[ -n "${paused_pod}" ]]; then
        signal_controller "${paused_pod}" CONT >/dev/null 2>&1 || true
    fi
    collect
}
trap finish EXIT

case "${IMAGE_MODE}" in
    import)
        k3d image import --cluster "${CLUSTER_NAME}" patroni-agent:latest patroni-controller:latest
        ;;
    skip)
        ;;
    *)
        fail "invalid image mode ${IMAGE_MODE}"
        ;;
esac
kubectl kustomize kubernetes/controller-agent-acceptance \
    --load-restrictor LoadRestrictionsNone | kubectl apply --filename -
kubectl --namespace "${NAMESPACE}" rollout status deployment/etcd-acceptance --timeout=180s
kubectl --namespace "${NAMESPACE}" rollout status statefulset/${STATEFULSET} --timeout=300s
kubectl --namespace "${NAMESPACE}" wait pod \
    --selector app.kubernetes.io/name=patroni \
    --for condition=Ready --timeout=300s

pod_total="$(kubectl --namespace "${NAMESPACE}" get pods \
    --selector app.kubernetes.io/name=patroni \
    --output 'jsonpath={.items[*].metadata.name}' | wc -w | tr -d ' ')"
[[ "${pod_total}" == "${POD_COUNT}" ]] || fail "expected ${POD_COUNT} Patroni Pods"

agent_identity="$(kubectl --namespace "${NAMESPACE}" exec "${TARGET_POD}" --container agent -- \
    sh -c 'printf "%s:%s:%s" "$(id -u)" "$(id -g)" "$(test ! -e /var/run/secrets/kubernetes.io/serviceaccount/token && echo no-token)"')"
[[ "${agent_identity}" == '999:999:no-token' ]] || fail "agent identity is ${agent_identity}"

controller_identity="$(kubectl --namespace "${NAMESPACE}" exec "${TARGET_POD}" --container controller -- \
    /usr/bin/python3 -c 'import os; print("%s:%s:%s" % (os.getuid(), os.getgid(), "no-token" if not os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token") else "token"))')"
[[ "${controller_identity}" == '65532:65532:no-token' ]] \
    || fail "controller identity is ${controller_identity}"

read -r dir_uid dir_gid dir_mode socket_uid socket_gid socket_mode <<<"$(
    kubectl --namespace "${NAMESPACE}" exec "${TARGET_POD}" --container agent -- \
        stat --format='%u %g %a' /run/patroni "${CONTROL_PATH}" | tr '\n' ' '
)"
[[ "${dir_uid}:${dir_gid}" == '0:999' ]] || fail "control directory owner is ${dir_uid}:${dir_gid}"
((8#${dir_mode} & 8#0020)) || fail "control directory is not group-writable"
((!(8#${dir_mode} & 8#0002))) || fail "control directory is world-writable"
[[ "${socket_uid}:${socket_gid}:${socket_mode}" == '999:999:660' ]] \
    || fail "control socket mode is ${socket_uid}:${socket_gid}:${socket_mode}"

primary_count=0
primary_pod=''
for ordinal in $(seq 0 $((POD_COUNT - 1))); do
    role="$(kubectl --namespace "${NAMESPACE}" exec "${STATEFULSET}-${ordinal}" --container agent -- \
        env PGPASSWORD="${ACCEPTANCE_PASSWORD}" psql --host 127.0.0.1 \
        --username postgres --dbname postgres --tuples-only --no-align \
        --command 'SELECT NOT pg_is_in_recovery()')"
    if [[ "${role}" == t ]]; then
        primary_count=$((primary_count + 1))
        primary_pod="${STATEFULSET}-${ordinal}"
    fi
done
[[ "${primary_count}" == 1 ]] || fail "expected one primary, got ${primary_count}"

# Authority expiry fences a primary while its controller is stopped.
primary_controller_before="$(kubectl --namespace "${NAMESPACE}" get pod "${primary_pod}" \
    --output 'jsonpath={.status.containerStatuses[?(@.name=="controller")].restartCount}')"
signal_controller "${primary_pod}" STOP
paused_pod="${primary_pod}"
for ((attempt = 0; attempt < FENCE_WAIT_ATTEMPTS; attempt++)); do
    if ! kubectl --namespace "${NAMESPACE}" exec "${primary_pod}" --container agent -- \
            pg_isready --host 127.0.0.1 --port 5432 >/dev/null 2>&1; then
        break
    fi
    sleep "${WAIT_SECONDS}"
done
if kubectl --namespace "${NAMESPACE}" exec "${primary_pod}" --container agent -- \
        pg_isready --host 127.0.0.1 --port 5432 >/dev/null 2>&1; then
    fail 'primary survived controller authority expiry'
fi
signal_controller "${primary_pod}" CONT
paused_pod=''
kubectl --namespace "${NAMESPACE}" wait pod "${primary_pod}" \
    --for condition=Ready --timeout=180s
primary_controller_after="$(kubectl --namespace "${NAMESPACE}" get pod "${primary_pod}" \
    --output 'jsonpath={.status.containerStatuses[?(@.name=="controller")].restartCount}')"
[[ "${primary_controller_after}" == "${primary_controller_before}" ]] \
    || fail 'kubelet restarted the stopped controller before fencing'

agent_before="$(restart_count agent)"
controller_before="$(restart_count controller)"
signal_controller "${TARGET_POD}" TERM >/dev/null 2>&1 || true
wait_restart controller "${controller_before}"
[[ "$(restart_count agent)" == "${agent_before}" ]] || fail 'controller restart restarted agent'

agent_before="$(restart_count agent)"
controller_before="$(restart_count controller)"
kubectl --namespace "${NAMESPACE}" exec "${TARGET_POD}" --container agent -- \
    rm -f "${CONTROL_PATH}"
wait_restart agent "${agent_before}"
[[ "$(restart_count controller)" == "${controller_before}" ]] || fail 'agent restart restarted controller'

echo 'controller-agent Kubernetes checks passed'
