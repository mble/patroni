#!/usr/bin/env bash
set -euo pipefail

readonly CONTROLLER_IMAGE="${CONTROLLER_IMAGE:-patroni-controller:latest}"
readonly CONTROLLER_BASE="${CONTROLLER_BASE:-gcr.io/distroless/python3-debian12:nonroot@sha256:7d1042ce588ab97019fe95c24ffca7bc5a82ccdac572511d5e09bda4435c89c5}"
readonly AGENT_IMAGE="${AGENT_IMAGE:-patroni-agent:latest}"
readonly AGENT_BASE="${AGENT_BASE:-postgres:18-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af}"
readonly REPORT_DIR="${1:-role-image-artifacts}"
readonly SEVERITIES='HIGH,CRITICAL'

scan_vulns() {
    local image="$1"
    local report="$2"

    trivy image --quiet --ignore-unfixed --severity "${SEVERITIES}" \
        --format json --output "${report}" "${image}"
}

scan_sbom() {
    local image="$1"
    local report="$2"

    trivy image --quiet --format cyclonedx --output "${report}" "${image}"
}

list_vulns() {
    local report="$1"

    jq -r '.Results[]?.Vulnerabilities[]? | select(.FixedVersion != "") | [.PkgName, .VulnerabilityID] | @tsv' \
        "${report}" | sort -u
}

check_delta() {
    local role="$1"
    local base_report="${REPORT_DIR}/${role}-base.json"
    local image_report="${REPORT_DIR}/${role}.json"
    local base_list="${REPORT_DIR}/${role}-base.txt"
    local image_list="${REPORT_DIR}/${role}.txt"
    local introduced="${REPORT_DIR}/${role}-introduced.txt"

    list_vulns "${base_report}" >"${base_list}"
    list_vulns "${image_report}" >"${image_list}"
    comm -13 "${base_list}" "${image_list}" >"${introduced}"

    if [[ ! -s "${introduced}" ]]; then
        return
    fi

    echo "${role} adds fixable high or critical vulnerabilities:" >&2
    cat "${introduced}" >&2
    exit 1
}

mkdir -p "${REPORT_DIR}"

scan_vulns "${CONTROLLER_BASE}" "${REPORT_DIR}/controller-base.json"
scan_vulns "${CONTROLLER_IMAGE}" "${REPORT_DIR}/controller.json"
scan_vulns "${AGENT_BASE}" "${REPORT_DIR}/agent-base.json"
scan_vulns "${AGENT_IMAGE}" "${REPORT_DIR}/agent.json"
scan_sbom "${CONTROLLER_IMAGE}" "${REPORT_DIR}/controller.cdx.json"
scan_sbom "${AGENT_IMAGE}" "${REPORT_DIR}/agent.cdx.json"

check_delta controller
check_delta agent
echo 'role image scans passed'
