# M01: Models and safety state machine

Status: complete, 2026-09-02

Implementation: `patroni/control/models.py`, `patroni/control/safety.py`, and
`tests/test_control.py`.

The state machine is pure. It returns `RUN`, `REJECT`, or `FENCE`; a later
driver layer performs I/O. Thirty-one focused tests cover every command kind,
authority and policy expiry, all command phases, idempotency, ordering, bounds,
and active versus paused loss.

## Goal

Prove the primary-authority invariant without real I/O.

## Work

1. Add immutable DTOs and enums.
2. Add agent, command, and authority states.
3. Inject a monotonic clock.
4. Validate boot IDs, terms, sequences, deadlines, and idempotency.
5. Represent active and paused Patroni policy explicitly.
6. Make active-mode fencing preempt every unsafe command.
7. Preserve paused-mode manual-control behaviour.
8. Bound command-result history.
9. Add a fake PostgreSQL driver.

## Tests

- Exhaustive allowed and forbidden transitions.
- Duplicate, reordered, stale, and conflicting commands.
- Expiry before, during, and after promotion.
- Primary discovery without authority.
- Initializer and failsafe authority expiry.
- Fence preemption at every command phase.
- Controller and DCS loss in active and paused modes.
- Dynamic TTL, retry, loop, and watchdog timing changes.

## Reviews

### Correctness

Every active-mode path producing or preserving a primary requires current
authority. Paused-mode outcomes match current Patroni. Failed validation has no
state-changing side effect.

### Security

DTOs carry no executable data. Invalid enums, sizes, identifiers, and deadlines
fail closed.

### Performance

Transitions are constant-time. Histories and payloads have named bounds. The
safety timer performs no polling loop.

## Exit

The pure state machine passes all invariant, parity, and transition tests.
