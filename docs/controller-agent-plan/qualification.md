# Split-mode qualification

Date: 2026-09-03

## Correctness

The full unit suite passes. Differential Unix tests compare direct and split
node observations. Extended local Jepsen runs used a 15-minute fault phase and
a two-minute final phase:

| Topology | PostgreSQL | Seed | Result |
|---|---:|---:|---|
| Split | 13 | 17 | Valid history; no lost, unexpected, or overlapping writes |
| Mixed | 17 | 43 | Valid history; no lost, unexpected, or overlapping writes |
| Split | 18 | 29 | Valid after live-postmaster health remediation |

The PostgreSQL 18 run first exposed a cached-state recovery defect. A regression
test failed before the controller health check was changed to query the live
postmaster. The identical campaign then passed. The final-revision CI matrix
repeats all three campaigns.

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

Matched PostgreSQL 18 containers on the M00 host produced these qualification
samples. They are not service-level objectives.

| Sample | Monolithic | Split |
|---|---:|---:|
| Idle replica Patroni PSS | 36.7 MiB | 75.6 MiB |
| Idle replica Patroni CPU | 0.31% core | 0.91% core |
| REST `/patroni` p50 | 1.210 ms | 1.878 ms |
| REST `/patroni` p95 | 1.658 ms | 2.939 ms |
| REST `/patroni` p99 | 1.965 ms | 9.017 ms |
| Synchronous `pgbench` | 759.8 TPS | 954.6 TPS |

The 5,000-call local control-socket sample measured 0.179 ms p50, 0.214 ms
p95, and 0.307 ms p99. Its p99 is below one percent of the one-second minimum
HA loop interval.

The split adds one controller interpreter and a small PID-1 supervisor. Removing
the supervisor's configuration imports reduced its PSS to 6.8 MiB. The remaining
memory, idle CPU, and REST costs require explicit acceptance before M10 closes.
