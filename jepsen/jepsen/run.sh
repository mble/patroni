#!/bin/bash

set -ex

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

readonly JEPSEN_RUN_TIMEOUT="${JEPSEN_RUN_TIMEOUT:-10800}"
readonly CLUSTER_WAIT_ATTEMPTS=60

echo "Jepsen seed: ${JEPSEN_SEED:-17}"

for n in {1..3}; do
    ssh-keyscan -t rsa "patroni$n" >> /root/.ssh/known_hosts
    ssh-keyscan -t rsa "etcd$n" >> /root/.ssh/known_hosts
done

for (( n=1; n <= CLUSTER_WAIT_ATTEMPTS; n++ )); do
    leader=$(ssh patroni1 "patronictl list -f json | jq -r '.[] | select(.Role==\"Leader\") | .Member'")
    [ -n "$leader" ] && break
    [ "$n" -eq "$CLUSTER_WAIT_ATTEMPTS" ] && exit 1
    sleep 1
done

# Strict mode blocks setup until one synchronous standby is ready.
for (( n=1; n <= CLUSTER_WAIT_ATTEMPTS; n++ )); do
    sync_count=$(ssh "$leader" "psql -U postgres -tAc \
        \"SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync', 'quorum')\"")
    [ "$sync_count" -gt 0 ] && break
    [ "$n" -eq "$CLUSTER_WAIT_ATTEMPTS" ] && exit 1
    sleep 1
done

ssh "$leader" "psql -U postgres -c 'CREATE TABLE IF NOT EXISTS set (value integer primary key)'"

for member in $(ssh "$leader" "patronictl list -f json | jq -r '.[] | select(.Role!=\"Leader\") | .Member'"); do
    for (( n=1; n <= CLUSTER_WAIT_ATTEMPTS; n++ )); do
        ssh "$member" "psql -U postgres -tAc 'SELECT * FROM set'" && break
        [ "$n" -eq "$CLUSTER_WAIT_ATTEMPTS" ] && exit 1
        sleep 1
    done
done

timeout "$JEPSEN_RUN_TIMEOUT" lein test
