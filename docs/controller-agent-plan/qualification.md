# Split-mode qualification

Date: 2026-09-03

## Correctness

The 885-test unit suite passes. Differential Unix tests compare direct and
split node observations. Final-revision local Jepsen runs used a 15-minute
fault phase and a two-minute final phase:

| Topology | PostgreSQL | Seed | Result |
|---|---:|---:|---|
| Split | 13 | 17 | Valid history; no lost, unexpected, or overlapping writes |
| Mixed | 17 | 43 | Valid history; no lost, unexpected, or overlapping writes |
| Split | 18 | 29 | Valid history; no lost, unexpected, or overlapping writes |

Qualification exposed cached health, stopped-primary observation, and missing
agent thread-pool defects. Each remediation added a failing regression test
before its fix. Clean images then passed all three campaigns and same-PGDATA
rollout tests. CI repeats this matrix.

The required CI matrix uses those durations and seeds. It retains histories,
checker output, DCS state, and process logs. The merge ruleset must require
every matrix job.

## Security

The example Pod shares only the control-socket volume. PGDATA and PostgreSQL
secrets are agent-only; etcd TLS credentials are controller-only. Service
account tokens are disabled. Both containers use UID/GID 999, read-only roots,
bounded writable volumes, resource limits, and no Linux capabilities.

The local socket checks peer credentials, owner, mode, path type, and inode.
Frames, collections, text, histories, workers, retries, and waits are bounded.
The Jepsen image installs this checkout. No new base image or external runtime
service is introduced.

The remaining boundary limitation is Pod-wide networking: NetworkPolicy cannot
deny unauthenticated agent traffic to etcd independently of the controller.

## Performance

Matched PostgreSQL 18 streaming replicas on the M00 host produced these
qualification samples. PSS is the sum of Patroni-side processes. Idle CPU is a
45-second process-time delta. REST results are 2,000 alternating requests from
one container. These samples are not service-level objectives.

| Sample | Monolithic | Split |
|---|---:|---:|
| Idle replica Patroni PSS | 38.2 MiB | 75.2 MiB |
| Idle replica Patroni CPU | 0.178% core | 0.733% core |
| REST `/patroni` p50 | 1.084 ms | 1.796 ms |
| REST `/patroni` p95 | 1.283 ms | 2.046 ms |
| REST `/patroni` p99 | 1.468 ms | 2.418 ms |
| Synchronous `pgbench` | 759.8 TPS | 954.6 TPS |

The 5,000-call local control-socket sample measured 0.179 ms p50, 0.214 ms
p95, and 0.307 ms p99. Its p99 is below one percent of the one-second minimum
HA loop interval.

The split adds one controller interpreter and a small PID-1 supervisor. Removing
the supervisor's configuration imports reduced its PSS to 6.8 MiB. The remaining
memory, idle CPU, and REST costs require explicit acceptance before M10 closes.
