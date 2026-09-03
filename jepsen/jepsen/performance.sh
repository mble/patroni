#!/usr/bin/env bash
set -euo pipefail

readonly SAMPLE_COUNT=30
readonly WARMUP_COUNT=3
readonly WAIT_COUNT=240
readonly WAIT_SECONDS=0.25
readonly CURL_TIMEOUT=30
readonly AUTH=admin:admin
readonly CLUSTER_URL=http://patroni1:8008/cluster
readonly RESULT_DIR=/tmp/patroni-performance
readonly NANOSECONDS_PER_MILLISECOND=1000000
readonly PERCENT_SCALE=100
readonly P50=50
readonly P95=95
readonly P99=99

mkdir -p "${RESULT_DIR}"

cluster() {
    curl --fail --silent --user "${AUTH}" "${CLUSTER_URL}"
}

wait_cluster() {
    local attempt
    local state

    for ((attempt = 0; attempt < WAIT_COUNT; attempt++)); do
        state="$(cluster)"
        if [[ "$(jq '[.members[] | select(.role == "leader")] | length' <<<"${state}")" == 1 ]] \
                && [[ "$(jq '[.members[] | select(.role == "quorum_standby" and .state == "streaming")] | length' \
                    <<<"${state}")" == 2 ]]; then
            return
        fi

        sleep "${WAIT_SECONDS}"
    done

    printf 'cluster did not recover\n' >&2
    return 1
}

current_leader() {
    cluster | jq --raw-output '.members[] | select(.role == "leader") | .name'
}

next_candidate() {
    local leader="$1"

    if [[ "${leader}" == patroni1 ]]; then
        printf 'patroni2\n'
        return
    fi

    printf 'patroni1\n'
}

request_body() {
    local action="$1"
    local leader="$2"
    local candidate="$3"

    if [[ "${action}" == switchover ]]; then
        jq --compact-output --null-input \
            --arg leader "${leader}" --arg candidate "${candidate}" \
            '{leader: $leader, candidate: $candidate}'
        return
    fi

    jq --compact-output --null-input --arg candidate "${candidate}" \
        '{candidate: $candidate}'
}

run_one() {
    local action="$1"
    local output="$2"
    local leader
    local candidate
    local body
    local response
    local start_ns
    local end_ns

    wait_cluster
    leader="$(current_leader)"
    candidate="$(next_candidate "${leader}")"
    body="$(request_body "${action}" "${leader}" "${candidate}")"

    start_ns="$(date +%s%N)"
    response="$(curl --fail --silent --show-error --max-time "${CURL_TIMEOUT}" \
        --user "${AUTH}" --header 'Content-Type: application/json' \
        --request POST --data "${body}" "http://${leader}:8008/${action}")"
    end_ns="$(date +%s%N)"

    if [[ "${response}" != Successfully* ]]; then
        printf '%s\n' "${response}" >&2
        return 1
    fi

    wait_cluster
    awk -v elapsed="$((end_ns - start_ns))" -v scale="${NANOSECONDS_PER_MILLISECOND}" \
        'BEGIN { printf "%.3f\n", elapsed / scale }' >>"${output}"
}

report() {
    local action="$1"
    local output="$2"
    local sorted="${output}.sorted"

    sort --numeric-sort "${output}" >"${sorted}"
    awk -v action="${action}" -v count="${SAMPLE_COUNT}" -v scale="${PERCENT_SCALE}" \
        -v p50_value="${P50}" -v p95_value="${P95}" -v p99_value="${P99}" '
        NR == 1 { minimum = $1 }
        NR == int((count * p50_value + scale - 1) / scale) { p50 = $1 }
        NR == int((count * p95_value + scale - 1) / scale) { p95 = $1 }
        NR == int((count * p99_value + scale - 1) / scale) { p99 = $1 }
        { total += $1; maximum = $1 }
        END {
            printf "%s n=%d min=%.3f p50=%.3f p95=%.3f p99=%.3f max=%.3f mean=%.3f ms\n",
                action, count, minimum, p50, p95, p99, maximum, total / count
        }
    ' "${sorted}"
}

run_set() {
    local action="$1"
    local output="${RESULT_DIR}/${action}.txt"
    local sample

    : >"${output}"

    for ((sample = 0; sample < WARMUP_COUNT; sample++)); do
        run_one "${action}" /dev/null
    done

    for ((sample = 1; sample <= SAMPLE_COUNT; sample++)); do
        run_one "${action}" "${output}"
        printf '%s %d/%d\n' "${action}" "${sample}" "${SAMPLE_COUNT}"
    done

    report "${action}" "${output}"
}

run_set switchover
run_set failover
