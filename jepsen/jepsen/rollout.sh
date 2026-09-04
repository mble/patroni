#!/bin/bash
# shellcheck disable=SC2029 # Fixed paths intentionally expand before SSH.

set -euo pipefail

readonly PATRONI_NODE_COUNT=3
readonly MEMBER_WAIT_ATTEMPTS=90
readonly SERVICE_WAIT_ATTEMPTS=30
readonly MONOLITH_SERVICE=/etc/patroni-services/patroni
readonly ACTIVE_MONOLITH=/etc/service/patroni
readonly KNOWN_HOSTS=/root/.ssh/known_hosts

touch "$KNOWN_HOSTS"

for ((index = 1; index <= PATRONI_NODE_COUNT; index++)); do
    node="patroni$index"
    if ! ssh-keygen -F "$node" -f "$KNOWN_HOSTS" >/dev/null; then
        ssh-keyscan -t rsa "$node" >> "$KNOWN_HOSTS"
    fi
done

pick_replica() {
    for ((attempt = 1; attempt <= MEMBER_WAIT_ATTEMPTS; attempt++)); do
        nodes=$(ssh patroni1 "patronictl list -f json" 2>/dev/null \
            | jq -r '.[] | select(.Role != "Leader" and .State == "streaming") | .Member' \
            || true)
        for node in $nodes; do
            if ssh "$node" test -d /etc/service/patroni-agent; then
                echo "$node"
                return
            fi
        done
        sleep 1
    done

    return 1
}

readonly NODE="${ROLLOUT_NODE:-$(pick_replica)}"

wait_member() {
    for ((attempt = 1; attempt <= MEMBER_WAIT_ATTEMPTS; attempt++)); do
        if ssh "$NODE" "psql -U postgres -tAc 'SELECT pg_is_in_recovery()'" \
                2>/dev/null | grep -qx t; then
            return
        fi
        sleep 1
    done

    return 1
}

wait_stopped() {
    for ((attempt = 1; attempt <= SERVICE_WAIT_ATTEMPTS; attempt++)); do
        if ! ssh "$NODE" pgrep -x postgres >/dev/null; then
            return
        fi
        sleep 1
    done

    return 1
}

start_monolith() {
    ssh "$NODE" ln -s "$MONOLITH_SERVICE" "$ACTIVE_MONOLITH"
    for ((attempt = 1; attempt <= SERVICE_WAIT_ATTEMPTS; attempt++)); do
        if ssh "$NODE" sv up "$ACTIVE_MONOLITH" >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done

    return 1
}

system_id() {
    ssh "$NODE" "psql -U postgres -tAc \
        'SELECT system_identifier FROM pg_control_system()'" | tr -d '[:space:]'
}

wait_member
SYSTEM_ID="$(system_id)"
readonly SYSTEM_ID
test -n "$SYSTEM_ID"

# Stop both split managers before exposing the monolithic service.
ssh "$NODE" 'sv down patroni-controller; sv down patroni-agent'
wait_stopped
start_monolith
wait_member
test "$(system_id)" = "$SYSTEM_ID"
ssh "$NODE" "pgrep -f \
    '^/usr/bin/python3 /usr/local/bin/patroni /home/postgres/patroni.yml$'" >/dev/null

# Stop monolithic Patroni before restoring the split managers.
ssh "$NODE" sv down "$ACTIVE_MONOLITH"
wait_stopped
ssh "$NODE" rm "$ACTIVE_MONOLITH"
ssh "$NODE" sv up patroni-agent
for ((attempt = 1; attempt <= SERVICE_WAIT_ATTEMPTS; attempt++)); do
    if ssh "$NODE" test -S /run/patroni/agent.sock; then
        break
    fi
    test "$attempt" -lt "$SERVICE_WAIT_ATTEMPTS"
    sleep 1
done
ssh "$NODE" 'sv up patroni-controller'
wait_member
test "$(system_id)" = "$SYSTEM_ID"
ssh "$NODE" "pgrep -f \
    '^/usr/bin/python3 /usr/local/bin/patroni-agent /home/postgres/agent.yml$'" >/dev/null

echo "Rollout and rollback preserved $NODE PGDATA"
