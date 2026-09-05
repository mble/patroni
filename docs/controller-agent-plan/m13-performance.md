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
