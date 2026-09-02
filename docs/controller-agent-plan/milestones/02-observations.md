# M02: Coherent observations

Status: complete, 2026-09-02

Implementation: `patroni/control/models.py`, `patroni/control/node.py`,
`patroni/control/postgres.py`, `patroni/api.py`, `patroni/ha.py`, and
`tests/test_control_node.py`.

HA and REST read local state through `NodeControl`. The in-process adapter
preserves existing read timing, retry points, REST output, and HA cache points.
Sixteen focused tests cover lifecycle states, failures, consistency, cache
invalidation, connection fallback, bounds, and redaction. Existing HA and API
tests remain differential parity tests.

## Goal

Remove direct controller reads of PostgreSQL internals.

## Work

1. Add `NodeSnapshot` and a local collector.
2. Add an in-process `NodeControl` adapter.
3. Route HA read decisions through snapshots.
4. Move REST status SQL into the collector.
5. Compose public REST output from agent and DCS state.
6. Separate observed role from desired role.
7. Add explicit freshness and retry modes matching current callers.
8. Reuse only values Patroni already caches within one HA cycle.

## Tests

- Primary, replica, stopped, starting, and failed-query snapshots.
- Current REST status and metrics parity.
- Request-time freshness and retry parity for every REST route.
- Concurrent state-change consistency.
- Stale and failed snapshot handling.
- Credential and configuration redaction.

## Reviews

### Correctness

HA reads preserve current refresh and cache points. Unknown and stale
observations never become affirmative health or promotion evidence.

### Security

Snapshots and logs contain no passwords, credential DSNs, environment, or raw
configuration.

### Performance

Avoid duplicate queries only where current Patroni already caches them. REST and
metrics retain request-time collection. Detailed replication data is requested
only when current policy needs it.

## Exit

HA and REST no longer access PostgreSQL connections or nested handlers.
