# M04: Recovery and configuration

Status: complete, 2026-09-02

Implementation: `patroni/control/recovery.py`,
`patroni/control/journal.py`, `patroni/control/commands.py`,
`patroni/control/postgres_commands.py`, `patroni/control/node.py`,
`patroni/ha.py`, `tests/test_control_recovery.py`, and
`tests/test_control_journal.py`.

Bootstrap, clone, rewind, crash recovery, reinitialize, PGDATA cleanup,
configuration application, checkpoints, and callbacks now use typed agent
commands. Recovery targets contain no credentials or paths. The agent merges
local credentials and bootstrap configuration, excluding DCS configuration.
The bounded terminal-result journal stores request hashes and public outcomes
only. Existing HA and recovery tests retain monolithic ordering and outcomes.

## Goal

Move destructive and filesystem-sensitive workflows into the agent.

## Work

1. Move bootstrap, clone, rewind, reinitialize, and crash recovery.
2. Add typed follow plans and divergence policies.
3. Move PostgreSQL configuration application and callbacks.
4. Add only the bounded command journal required for IPC idempotency.
5. Never journal grants, secrets, or new recovery policy.
6. Reproduce current `/initialize`, PGDATA cleanup, and restart outcomes.
7. Merge public upstream targets with agent-held credentials.
8. Preserve documented callback and replica-method arguments and ordering.

## Tests

- Crash at every bootstrap, rewind, and reinitialize phase.
- Controller-only and whole-Pod restart during initialization.
- Differential bootstrap recovery from empty, partial, and valid PGDATA.
- Invalid and mismatched system identifiers.
- Failed callbacks and partial configuration writes.
- PGDATA traversal, symlink, and ownership attacks.
- Journal corruption and incompatible versions.

## Reviews

### Correctness

Interrupted destructive work follows current Patroni outcomes. The journal
prevents duplicate IPC execution but does not add automatic finish, retry,
rollback, or cleanup behaviour.

### Security

All paths remain beneath configured roots. Files use restrictive permissions.
Commands and replica methods remain allowlisted. Callback and replica-method
processes cannot receive controller-only secrets. Errors redact credentials.

### Performance

Configuration hashes avoid rewrites. Basebackup and rewind output stays local.
Journal growth and fsync frequency are bounded.

## Exit

HA has no bootstrap, rewind, configuration, callback, or cancellable access,
with monolithic recovery and script-contract parity.
