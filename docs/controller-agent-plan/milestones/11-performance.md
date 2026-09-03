# M11: split-process performance

Status: complete, 2026-09-03

## Objective

Attribute the accepted split-mode overhead and remove avoidable transport,
polling, and supervision costs without changing Patroni semantics.

## Changes

- Reuse the authenticated controller-agent connection.
- Schedule authority checks at their exact safety deadline.
- Use native `dumb-init` as container PID 1.
- Retain `patroni-agent-supervisor` as a Python fallback.

The agent remains the sole PostgreSQL lifecycle authority. PID-file discovery,
orphan adoption, graceful shutdown, and fail-closed authority rules are
unchanged.

## Results

Fresh matched PostgreSQL 18 measurements reduced split PSS from 77.7 to
69.9 MiB, idle CPU from 0.733% to 0.500% core, and REST p95 from 2.046 to
1.716 ms. Split mode remains above monolithic mode because it requires two
Python runtimes and a cross-process REST snapshot.

Unit, static, manifest, documentation, and short Jepsen regression gates pass.
The full Jepsen matrix remains required by CI.
