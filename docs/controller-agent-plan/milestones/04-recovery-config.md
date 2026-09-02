# M04: Recovery and configuration

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
