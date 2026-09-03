# Split-mode qualification

Date: 2026-09-03

## Correctness

The 889-test unit suite passes. Differential Unix tests compare direct and
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
qualification samples. Memory is five summed Patroni-process readings. Idle
CPU is a 45-second process-time delta. REST uses 2,000 alternating requests
from one container. These samples are not service-level objectives.

| Sample | Monolithic | Split |
|---|---:|---:|
| Idle replica Patroni RSS | 41.7 MiB | 81.7 MiB |
| Idle replica Patroni PSS | 39.6 MiB | 77.7 MiB |
| Idle replica Patroni CPU | 0.178% core | 0.733% core |
| REST `/patroni` p50 | 1.084 ms | 1.796 ms |
| REST `/patroni` p95 | 1.283 ms | 2.046 ms |
| REST `/patroni` p99 | 1.468 ms | 2.418 ms |
| Synchronous `pgbench` | 759.8 TPS | 954.6 TPS |

Debug logs measured each replica HA cycle from lock observation through its
result. Three warmups preceded 30 transition samples per pure topology. Every
transition waited for one leader and two streaming quorum standbys before the
next request.

| Sample | Monolithic | Split | Change | Gate |
|---|---:|---:|---:|---|
| Replica HA cycle p95 | 125 ms | 133 ms | +6.4% | Pass |
| Switchover p95 | 3,081.450 ms | 3,078.330 ms | -0.1% | Pass |
| Failover p95 | 3,080.702 ms | 3,081.058 ms | +0.01% | Pass |
| REST `/patroni` p95 | 1.283 ms | 2.046 ms | +59.5% | Accepted |
| Idle Patroni RSS | 41.7 MiB | 81.7 MiB | +95.9% | Accepted |
| Idle Patroni CPU | 0.178% | 0.733% | +311.8% | Accepted |

| Operation | Topology | n | Min (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) | Mean (ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Switchover | Monolithic | 30 | 2,064.504 | 3,068.699 | 3,081.450 | 3,092.904 | 3,092.904 | 2,674.079 |
| Switchover | Split | 30 | 2,027.523 | 3,069.927 | 3,078.330 | 3,120.237 | 3,120.237 | 2,771.999 |
| Failover | Monolithic | 30 | 2,065.160 | 3,071.573 | 3,080.702 | 3,081.802 | 3,081.802 | 2,707.430 |
| Failover | Split | 30 | 2,063.512 | 3,074.250 | 3,081.058 | 3,084.196 | 3,084.196 | 2,874.235 |

The 5,000-call local control-socket sample measured 0.179 ms p50, 0.214 ms
p95, and 0.307 ms p99. Its p99 is below one percent of the one-second minimum
HA loop interval.

The split adds one controller interpreter and a PID-1 supervisor. HA-cycle,
failover, and switchover gates pass. Memory, idle CPU, and REST costs were
accepted for this fork on 2026-09-03.

### Performance pass

A fresh matched run reused authenticated control connections, scheduled
authority checks at their deadlines, and replaced the Python PID-1 process with
`dumb-init`. CPU is a 120-second process-time delta. PSS is summed from
`smaps_rollup`. REST values are medians of five 1,000-call rounds.

| Sample | Monolithic | Split | Change |
|---|---:|---:|---:|
| Idle Patroni PSS | 39.1 MiB | 69.9 MiB | +79% |
| Idle Patroni CPU | 0.292% core | 0.500% core | +71% |
| REST `/patroni` p50 | 0.843 ms | 1.332 ms | +58% |
| REST `/patroni` p95 | 1.181 ms | 1.716 ms | +45% |

Compared with the accepted split samples, PSS fell 10%, CPU fell 31.8%, and
REST p95 fell 16.1%. Native PID 1 used about 0.1 MiB PSS instead of 7.3 MiB.
The remaining memory cost is the second Patroni Python interpreter. REST keeps
a required cross-process snapshot, codec, and context switch. CPU retains two
runtimes plus controller policy and agent lifecycle work. Removing those costs
would require a native agent or protocol redesign, not another supervisor
change.

A two-minute split Jepsen regression with process and network faults remained
valid: no lost or unexpected writes and no overlapping writable primaries. It
supplements, but does not replace, the required 15-minute CI campaign.
