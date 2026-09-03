#!/usr/bin/env bash
set -euo pipefail

readonly MIB_BYTES=1048576
readonly CONTROLLER_MAX_MIB=150
readonly AGENT_MAX_OVERHEAD_MIB=75
readonly CONTROLLER_UID='65532:65532'
readonly AGENT_UID='999:999'
readonly CONTROLLER_PORTS='{"8008/tcp":{}}'
readonly AGENT_PORTS='{"5432/tcp":{}}'
readonly CONTROLLER_ENTRY='["/usr/bin/python3","-m","patroni.controller"]'
readonly AGENT_ENTRY='["/usr/bin/dumb-init","--single-child","--","/usr/bin/python3","-m","patroni.agent"]'
readonly CONTROLLER_DISTS='dnspython,patroni,psutil,python-dateutil,python-etcd,pyyaml,six,urllib3'
readonly AGENT_DISTS='patroni,psutil,psycopg,psycopg-binary,python-dateutil,pyyaml,six,typing-extensions,urllib3'
readonly AGENT_OS_DELTA='dumb-init
libexpat1
libnsl2
libpython3-stdlib
libpython3.11-minimal
libpython3.11-stdlib
libtirpc-common
libtirpc3
media-types
python3
python3-minimal
python3.11
python3.11-minimal'
readonly CONTROLLER_IMAGE="${CONTROLLER_IMAGE:-patroni-controller:latest}"
readonly AGENT_IMAGE="${AGENT_IMAGE:-patroni-agent:latest}"
readonly AGENT_BASE="${AGENT_BASE:-postgres:18-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af}"

fail() {
    echo "role image check: $*" >&2
    exit 1
}

inspect() {
    docker image inspect --format "$1" "$2"
}

ensure_base() {
    if docker image inspect "${AGENT_BASE}" >/dev/null 2>&1; then
        return
    fi

    docker pull "${AGENT_BASE}" >/dev/null
}

check_eq() {
    local expected="$1"
    local actual="$2"
    local label="$3"

    if [[ "${actual}" == "${expected}" ]]; then
        return
    fi

    fail "${label}: expected ${expected}, got ${actual}"
}

list_dists() {
    local image="$1"
    local python="$2"

    docker run --rm --entrypoint "${python}" "${image}" -c \
        'from importlib.metadata import distributions; print(",".join(sorted({d.metadata["Name"].lower().replace("_", "-") for d in distributions()})))'
}

list_pkgs() {
    local image="$1"

    docker run --rm --entrypoint /usr/bin/dpkg-query "${image}" \
        -W '-f=${binary:Package}\n' | sed 's/:.*$//' | sort -u
}

check_size() {
    local controller_size
    local agent_size
    local base_size
    local controller_max
    local agent_max

    controller_size="$(inspect '{{.Size}}' "${CONTROLLER_IMAGE}")"
    agent_size="$(inspect '{{.Size}}' "${AGENT_IMAGE}")"
    base_size="$(inspect '{{.Size}}' "${AGENT_BASE}")"
    controller_max=$((CONTROLLER_MAX_MIB * MIB_BYTES))
    agent_max=$((base_size + AGENT_MAX_OVERHEAD_MIB * MIB_BYTES))

    if ((controller_size > controller_max)); then
        fail "controller exceeds ${CONTROLLER_MAX_MIB} MiB"
    fi
    if ((agent_size > agent_max)); then
        fail "agent overhead exceeds ${AGENT_MAX_OVERHEAD_MIB} MiB"
    fi

    echo "controller bytes: ${controller_size}"
    echo "agent bytes: ${agent_size}"
    echo "agent base bytes: ${base_size}"
}

check_image() {
    check_eq "${CONTROLLER_UID}" "$(inspect '{{.Config.User}}' "${CONTROLLER_IMAGE}")" 'controller user'
    check_eq "${AGENT_UID}" "$(inspect '{{.Config.User}}' "${AGENT_IMAGE}")" 'agent user'
    check_eq "${CONTROLLER_PORTS}" "$(inspect '{{json .Config.ExposedPorts}}' "${CONTROLLER_IMAGE}")" 'controller ports'
    check_eq "${AGENT_PORTS}" "$(inspect '{{json .Config.ExposedPorts}}' "${AGENT_IMAGE}")" 'agent ports'
    check_eq "${CONTROLLER_ENTRY}" "$(inspect '{{json .Config.Entrypoint}}' "${CONTROLLER_IMAGE}")" 'controller entry point'
    check_eq "${AGENT_ENTRY}" "$(inspect '{{json .Config.Entrypoint}}' "${AGENT_IMAGE}")" 'agent entry point'
    check_eq "${CONTROLLER_DISTS}" "$(list_dists "${CONTROLLER_IMAGE}" /usr/bin/python3)" 'controller distributions'
    check_eq "${AGENT_DISTS}" "$(list_dists "${AGENT_IMAGE}" /usr/bin/python3)" 'agent distributions'

    if docker run --rm --entrypoint /bin/sh "${CONTROLLER_IMAGE}" -c true >/dev/null 2>&1; then
        fail 'controller contains a shell'
    fi

    docker run --rm --read-only "${CONTROLLER_IMAGE}" --help >/dev/null
    docker run --rm --read-only --tmpfs /tmp "${AGENT_IMAGE}" --help >/dev/null
}

check_imports() {
    docker run --rm --entrypoint /usr/bin/python3 "${CONTROLLER_IMAGE}" -c \
        'import etcd; from patroni.dcs.etcd3 import Etcd3' \
        >/dev/null
    docker run --rm --entrypoint /usr/bin/python3 "${AGENT_IMAGE}" -c \
        'import psycopg; from patroni.postgresql import Postgresql' \
        >/dev/null

    if docker run --rm --entrypoint /usr/bin/python3 "${CONTROLLER_IMAGE}" \
        -c 'import psycopg' >/dev/null 2>&1; then
        fail 'controller imports psycopg'
    fi
    if docker run --rm --entrypoint /usr/bin/python3 "${AGENT_IMAGE}" \
        -c 'import etcd' >/dev/null 2>&1; then
        fail 'agent imports a DCS client'
    fi
}

check_os_delta() {
    local base_packages
    local agent_packages
    local added_packages

    base_packages="$(list_pkgs "${AGENT_BASE}")"
    agent_packages="$(list_pkgs "${AGENT_IMAGE}")"
    added_packages="$(comm -13 \
        <(printf '%s\n' "${base_packages}") \
        <(printf '%s\n' "${agent_packages}"))"

    check_eq "${AGENT_OS_DELTA}" "${added_packages}" 'agent OS package delta'
}

ensure_base
check_size
check_image
check_imports
check_os_delta
echo 'role image checks passed'
