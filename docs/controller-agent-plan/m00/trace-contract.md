# Differential trace contract

Monolithic and split runs emit normalized events at the same logical boundary.
The trace compares behavior, not implementation calls.

## Event

```text
TraceEvent
  sequence
  monotonic_time
  actor                 controller | agent | postgres | dcs | rest
  kind
  operation_id
  authority_kind
  authority_epoch
  role_before
  role_after
  dcs_path
  dcs_action
  postgres_action
  result
```

Fields absent for an event remain absent. Payloads use enums and bounded domain
values. Credentials, SQL, paths containing secrets, PIDs, log text, wall-clock
time, and Patroni objects are excluded.

## Comparison

Normalize member-specific addresses, monotonic origins, operation IDs, and
epochs. Preserve event order. Compare:

- exact DCS mutation sequence;
- lifecycle and role transition sequence;
- authority proof before primary-producing work;
- shutdown safepoint before leader deletion;
- REST request, response code, and normalized body;
- callback and replica-method invocation sequence;
- watchdog transitions;
- terminal state from the parity matrix.

Concurrent read-only observations may commute. DCS writes, authority changes,
fences, PostgreSQL mutations, callbacks, and REST mutations may not.

## Scenarios

Capture bootstrap, steady leader renewal, promotion, failover, immediate and
scheduled switchover, DCS outage with and without failsafe, pause and resume,
restart, reinitialize, rewind, shutdown, controller loss, agent loss, socket
loss, postmaster loss, and process adoption.

## Performance gates

Matched runs use at least 30 samples after warm-up.

| Name | Gate |
|---|---|
| `SAFETY_DEADLINE_EXTENSION` | zero |
| `HA_CYCLE_P95_REGRESSION` | at most 10% |
| `REST_GET_P95_REGRESSION` | at most 10% |
| `FAILOVER_P95_REGRESSION` | at most 10% |
| `SWITCHOVER_P95_REGRESSION` | at most 10% |
| `CONTROLLER_AGENT_RSS_REGRESSION` | at most 25% over monolithic RSS |
| `CONTROLLER_AGENT_CPU_REGRESSION` | at most 25% over monolithic CPU time |

The safety gate overrides every latency allowance. IPC waits and retries remain
inside current `retry_timeout`, TTL, and watchdog deadlines.

Threshold changes require measured evidence and approval.
