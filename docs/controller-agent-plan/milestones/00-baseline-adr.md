# M00: Baseline and ADR

## Goal

Freeze decisions and establish reproducible behaviour before refactoring.

## Work

1. Create `controller-agent` from `5f2c94c8`.
2. Add the formal ADR from this plan.
3. Record unit, lint, type, docs, and selected `etcd3` behaviour results.
4. Record HA-cycle, status-query, failover, and switchover baselines.
5. Build a semantic-parity matrix for HA, REST, bootstrap, callbacks, pause,
   watchdog, shutdown, and process adoption.
6. Record any unavoidable difference for separate approval before coding it.

## Tests and evidence

- Existing test baseline is reproducible.
- No product behaviour changes.
- Known baseline failures are recorded, not silently accepted.
- Monolithic and future split traces have defined comparison points.

## Reviews

### Correctness

Trace bootstrap, promotion, demotion, failsafe, pause, restart, reinitialize,
shutdown, safepoints, and process failure. Record the current outcome as the
required split outcome.

### Security

List trusted processes, secret owners, mounts, socket access, and excluded
threats. Confirm the controller is trusted, not Byzantine-resistant.

### Performance

Record commands, environment, hardware, and distributions rather than single
timings. Define comparison thresholds before implementation.

## Exit

The ADR has no unresolved parity or safety decision and the baseline is
repeatable.
