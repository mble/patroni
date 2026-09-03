#!/usr/bin/env bash
set -euo pipefail

readonly WAIT_ATTEMPTS=120
readonly WAIT_SECONDS=1
readonly ETCD_TIMEOUT=1s
readonly POSTGRES_PORT=55432
readonly REST_PORT=58008
REPO_DIR="$(dirname "$(dirname "$(dirname "$0")")")"
readonly REPO_DIR

export PYTHONPATH="${REPO_DIR}${PYTHONPATH:+:${PYTHONPATH}}"

RUN_DIR="$(mktemp -d -t patroni-controller-agent.XXXXXX)"
readonly RUN_DIR
readonly SOCKET_PATH="${RUN_DIR}/agent.sock"
readonly DATA_DIR="${RUN_DIR}/data"
readonly AGENT_CONFIG="${RUN_DIR}/agent.yml"
readonly CONTROLLER_CONFIG="${RUN_DIR}/controller.yml"
readonly AGENT_LOG="${RUN_DIR}/agent.log"
readonly CONTROLLER_LOG="${RUN_DIR}/controller.log"
readonly ETCD_LOG="${RUN_DIR}/etcd.log"

agent_pid=''
controller_pid=''
etcd_pid=''

cleanup() {
    if [[ -n "${controller_pid}" ]]; then
        kill "${controller_pid}" 2>/dev/null || true
        wait "${controller_pid}" 2>/dev/null || true
    fi
    if [[ -n "${agent_pid}" ]]; then
        kill "${agent_pid}" 2>/dev/null || true
        wait "${agent_pid}" 2>/dev/null || true
    fi
    if [[ -n "${etcd_pid}" ]]; then
        kill "${etcd_pid}" 2>/dev/null || true
        wait "${etcd_pid}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

mkdir -p "${DATA_DIR}"

if [[ "$(uname -m)" == 'aarch64' ]]; then
    export ETCD_UNSUPPORTED_ARCH=arm64
fi

etcd --data-dir "${RUN_DIR}/etcd" \
    --advertise-client-urls http://127.0.0.1:2379 \
    --listen-client-urls http://127.0.0.1:2379 >"${ETCD_LOG}" 2>&1 &
etcd_pid=$!

for ((attempt = 0; attempt < WAIT_ATTEMPTS; attempt++)); do
    if ETCDCTL_API=3 etcdctl --dial-timeout="${ETCD_TIMEOUT}" \
        --command-timeout="${ETCD_TIMEOUT}" endpoint health >/dev/null 2>&1; then
        break
    fi
    sleep "${WAIT_SECONDS}"
done
if ! ETCDCTL_API=3 etcdctl --dial-timeout="${ETCD_TIMEOUT}" \
    --command-timeout="${ETCD_TIMEOUT}" endpoint health >/dev/null 2>&1; then
    cat "${ETCD_LOG}"
    exit 1
fi

cat >"${AGENT_CONFIG}" <<EOF
scope: controller-agent-smoke
name: node-a
postgresql:
  listen: 127.0.0.1:${POSTGRES_PORT}
  connect_address: 127.0.0.1:${POSTGRES_PORT}
  data_dir: ${DATA_DIR}
  pgpass: ${RUN_DIR}/pgpass
  authentication:
    replication:
      username: replicator
      password: rep-pass
    superuser:
      username: postgres
      password: postgres
  parameters:
    unix_socket_directories: ${RUN_DIR}
bootstrap:
  initdb:
    - encoding: UTF8
    - data-checksums
  pg_hba:
    - host all all 127.0.0.1/32 trust
    - host replication replicator 127.0.0.1/32 trust
watchdog:
  mode: off
agent:
  socket: ${SOCKET_PATH}
  socket_mode: 384
  timeout: 5
  max_workers: 16
EOF

cat >"${CONTROLLER_CONFIG}" <<EOF
scope: controller-agent-smoke
name: node-a
restapi:
  listen: 127.0.0.1:${REST_PORT}
  connect_address: 127.0.0.1:${REST_PORT}
etcd3:
  host: 127.0.0.1:2379
postgresql:
  connect_address: 127.0.0.1:${POSTGRES_PORT}
controller:
  socket: ${SOCKET_PATH}
  timeout: 5
watchdog:
  mode: off
bootstrap:
  dcs:
    ttl: 20
    loop_wait: 5
    retry_timeout: 5
    postgresql:
      use_slots: true
EOF

python3 -m patroni.agent "${AGENT_CONFIG}" >"${AGENT_LOG}" 2>&1 &
agent_pid=$!

for ((attempt = 0; attempt < WAIT_ATTEMPTS; attempt++)); do
    if [[ -S "${SOCKET_PATH}" ]]; then
        break
    fi
    if ! kill -0 "${agent_pid}" 2>/dev/null; then
        cat "${AGENT_LOG}"
        exit 1
    fi
    sleep "${WAIT_SECONDS}"
done
if [[ ! -S "${SOCKET_PATH}" ]]; then
    cat "${AGENT_LOG}"
    exit 1
fi

python3 -m patroni.controller "${CONTROLLER_CONFIG}" >"${CONTROLLER_LOG}" 2>&1 &
controller_pid=$!

for ((attempt = 0; attempt < WAIT_ATTEMPTS; attempt++)); do
    if curl --fail --silent "http://127.0.0.1:${REST_PORT}/primary" >/dev/null; then
        break
    fi
    if ! kill -0 "${controller_pid}" 2>/dev/null; then
        cat "${AGENT_LOG}"
        cat "${CONTROLLER_LOG}"
        exit 1
    fi
    sleep "${WAIT_SECONDS}"
done

if ! curl --fail --silent "http://127.0.0.1:${REST_PORT}/primary" >/dev/null; then
    cat "${AGENT_LOG}"
    cat "${CONTROLLER_LOG}"
    exit 1
fi

psql --host=127.0.0.1 --port="${POSTGRES_PORT}" --username=postgres \
    --dbname=postgres --tuples-only --command='SELECT NOT pg_is_in_recovery()' \
    | grep --quiet t

curl --fail --silent "http://127.0.0.1:${REST_PORT}/metrics" \
    | grep --quiet 'patroni_agent_connected.* 1.0'

echo 'controller-agent smoke passed'
