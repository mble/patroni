# M05: Replication and watchdog

Status: complete, 2026-09-02

Implementation: `patroni/control/replication.py`,
`patroni/control/models.py`, `patroni/control/commands.py`,
`patroni/control/node.py`, `patroni/control/postgres_commands.py`,
`patroni/postgresql/slots.py`, `patroni/ha.py`,
`tests/test_control_replication.py`, and existing sync, slot, quorum, HA,
watchdog, and safety suites.

The controller computes bounded sync and slot plans from DCS state. The agent
applies local PostgreSQL mechanics, adds local credentials, and returns bounded
slot-copy names. Watchdog activation, keepalive, disable, and ordered timing
reloads use `NodeControl`; keepalive bypasses the long-command worker. The full
non-Raft suite passes with 782 tests.

## Goal

Separate controller replication policy from agent-local mechanics and fencing.

## Work

1. Add controller-owned sync and slot plans.
2. Apply plans and observe readiness in the agent.
3. Update DCS sync state only after agent acknowledgement.
4. Move watchdog activation, keepalive, reload, and disable to the agent.
5. Reject promotion when a required watchdog cannot activate.
6. Preserve current TTL, loop, retry, safety-margin, and pause calculations.
7. Carry dynamic timing changes with a version and apply them in Patroni order.

## Tests

- Asynchronous, synchronous, and quorum modes.
- Slot creation, advancement, copying, and retention.
- Required, optional, unavailable, and non-disableable watchdogs.
- Authority expiry while watchdog is active.
- DCS loss and shutdown while paused.
- Dynamic timing reload during primary operation.
- Existing slot, sync, quorum, and watchdog suites.

## Reviews

### Correctness

DCS sync state follows confirmed local state. Watchdog activation, keepalive,
reload, pause, demotion, and disable traces match current Patroni.

### Security

Validate slot names, GUC values, replica identities, and target addresses.
Agent status exposes no replication credentials.

### Performance

Normal snapshots exclude unnecessary replication detail. Slot and sync work is
incremental and cannot starve grant handling.

## Exit

All PostgreSQL and host-local mechanics are behind `NodeControl`, with slot,
sync, and watchdog parity.
