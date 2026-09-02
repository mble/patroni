# M08: Controller mode and REST parity

## Goal

Run HA policy and public APIs without local PostgreSQL access.

## Work

1. Add `patroni-controller`.
2. Start only DCS, HA, peer requests, dynamic configuration, and public REST.
3. Add separate controller configuration validation.
4. Translate existing leader, initializer, failsafe, and pause decisions into
   agent policy without changing those decisions.
5. Stop renewal when active-mode agent observations become stale.
6. Preserve current clean shutdown and paused-shutdown actions through IPC.
7. Add composite readiness and agent metrics without changing existing metrics.
8. Remove the `psycopg` requirement from the controller entry point.
9. Preserve external REST and DCS formats, freshness, and status codes.

## Metrics

- Agent connection state.
- Snapshot age.
- Remaining authority time.
- Active command and phase.
- Fence count and reason.
- Negotiated protocol version.

## Tests

- Existing HA and REST suites through fake and real agents.
- Successful and failed DCS renewal.
- Agent loss while primary and replica.
- Failsafe grant creation and expiry.
- Paused DCS loss and controller shutdown.
- Controller reload and restart.
- Restart, reinitialize, failover, and switchover endpoints.
- Mixed monolithic and split members.
- Differential HA, DCS, REST, and shutdown traces.

## Reviews

### Correctness

Active authority never exceeds Patroni's existing evidence deadline. Paused
policy preserves existing exceptions. Stale or absent agent state cannot invent
membership or promotion evidence.

### Security

Controller mounts neither PGDATA nor PostgreSQL secrets. REST handlers can only
submit typed operations.

### Performance

HA uses bounded local RPCs within existing deadlines. REST and metrics preserve
current PostgreSQL query freshness.

## Exit

A local two-process node reaches HA, API, and lifecycle parity with etcd3.
