# M03: Lifecycle commands

## Goal

Move PostgreSQL lifecycle mutations behind an asynchronous command boundary.

## Work

1. Add one-at-a-time command execution.
2. Migrate start, stop, restart, promote, follow, cancel, and fence.
3. Return checkpoint and previous WAL positions from stop.
4. Replace shutdown callables with sequenced typed safepoint events.
5. Preserve current early leader-release checks and fallback ordering.
6. Track long actions by command ID.
7. Preserve public restart, demotion, shutdown, and switchover behaviour.

## Tests

- Lifecycle parity with existing unit tests.
- Cancellation during every phase.
- Authority loss during promotion and restart.
- Slow, failed, and partial shutdown.
- Safepoint delivery, acknowledgement, duplication, delay, and loss.
- Leader-key release trace parity.
- Duplicate submissions and result replay.
- Controller restart while a command remains active.

## Reviews

### Correctness

A safepoint or stopped result carries the same evidence current callbacks use.
DCS-key release conditions and timing preserve Patroni ordering. Agent loss
never invents evidence or causes early deletion.

### Security

Follow targets are validated records. No operation accepts raw SQL, shell text,
environment, or filesystem paths from the controller.

### Performance

Long actions do not block status, grants, cancellation, or fencing. Command
results and progress remain bounded.

## Exit

Controller HA code invokes no PostgreSQL lifecycle methods, and lifecycle traces
match monolithic Patroni.
