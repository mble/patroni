# M13 matched performance attribution

Date: 2026-09-05

## Control

M11 revision `2d4df83e` and M13 revision `ddeadb92` used the same cached
runtime layers:

- PostgreSQL 18.6, image digest
  `sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`;
- Python 3.13.5 on Linux arm64;
- identical dependency inventory, SHA-256
  `2b672aa250ff56c189445fcd4a46f8e76703e3c76a236ebb376aa6051ca4eecb`;
- OrbStack 2.2.3 and Docker 29.4.0 on the same M1 host.

Idle and REST tests ran M11, M13, then M11 again. PSS is the sum of the agent
and controller `smaps_rollup` values. CPU is a 120-second process-time delta.
REST is the median of five 1,000-call rounds from a peer container. Transition
tests used three warm-ups and 30 measured operations per revision.

Raw samples are retained in
`/private/tmp/patroni-perf-attribution-results`.

## Results

| Metric | M11 | M13 | M11 repeat | Result |
|---|---:|---:|---:|---|
| Idle PSS median | 69.179 MiB | 69.040 MiB | 67.945 MiB | Within M11 spread |
| Idle CPU | 0.450% | 0.400% | 0.425% | Improved |
| REST p50 | 0.863 ms | 0.812 ms | 0.818 ms | No regression |
| REST p95 | 1.008 ms | 0.931 ms | 0.932 ms | No regression |

| Metric | M11 | M13 | Change |
|---|---:|---:|---:|
| Replica HA-cycle p95 | 164 ms | 139 ms | -15.24% |
| Switchover p95 | 3,090.738 ms | 3,098.720 ms | +0.26% |
| Failover p95 | 3,095.718 ms | 3,091.474 ms | -0.14% |

The unmatched 77.0 MiB PSS and 2.836 ms REST p95 results were environmental.
No performance regression is attributable to the M11–M13 code range.

## Write-availability RTO

A client attempted one autocommit insert against every member every 20 ms.
The probe recorded successful commit responses, DCS leader observations, and
REST completion. Five planned switchovers and three leader-container crashes
ran against M13 with PostgreSQL 18.6.

| Transition | Samples | Interval | Median | Range |
|---|---:|---|---:|---:|
| Switchover: last old write to first new write | 5 | SQL gap | 5,455.974 ms | 5,426.980–5,492.569 ms |
| Switchover: request to DCS leader change | 5 | Control plane | 2,737.646 ms | 2,692.424–2,824.965 ms |
| Switchover: request to REST completion | 5 | API | 3,085.832 ms | 3,083.450–3,092.492 ms |
| Switchover: DCS change to first new write | 5 | Promotion | 3,052.471 ms | 2,973.731–3,053.801 ms |
| Switchover: REST completion to first new write | 5 | API lag | 2,701.713 ms | 2,662.775–2,706.203 ms |
| Crash: last old write to first new write | 3 | SQL gap | 34,430.340 ms | 32,693.683–34,550.654 ms |
| Crash: last old write to DCS leader change | 3 | Lease expiry | 32,294.411 ms | 30,478.188–32,445.917 ms |
| Crash: DCS change to first new write | 3 | Promotion | 2,135.929 ms | 2,104.737–2,215.496 ms |

REST success reports the DCS leader change, not write availability. Planned
switchover therefore returned about 2.7 seconds before the first confirmed new
write. Crash RTO is dominated by the default 30-second leader TTL. No sample
observed a successful old-leader write after the first new-leader write.

These measurements characterize the default configuration; M13 has no stated
write-RTO acceptance limit. Raw samples are retained in
`/private/tmp/patroni-rto-switchover.csv` and
`/private/tmp/patroni-rto-crash.csv`.
