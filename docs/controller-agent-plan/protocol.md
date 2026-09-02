# Control protocol

## Transport

- Linux `AF_UNIX`.
- One request and response per connection.
- Fixed header: magic, major version, minor version, payload length.
- Network byte order.
- Bounded UTF-8 JSON body.
- Strict operation-specific validation.
- Socket permissions and `SO_PEERCRED` checks on both sides.
- Explicit errors; never return exception representations.
- Adjacent minor versions remain compatible. Major mismatches fail.

The server bounds frame size, workers, deadlines, command history, and result
size. Stale-socket cleanup first verifies path type, ownership, and failed
connection. Never use `pickle`.

## Envelope

Every request contains:

- Request ID.
- Operation enum.
- Controller boot ID.
- Agent boot ID after handshake.
- Monotonic sequence.
- Typed body.

Every command additionally contains its command ID and required authority.

## Operations

- `HELLO`
- `STATUS`
- `GRANT`
- `POLICY`
- `SUBMIT`
- `COMMAND_STATUS`
- `EVENTS`
- `CANCEL`
- `FENCE`

An explicit `FENCE` request is always admissible and preempts active work.
Automatic authority-expiry fencing is disabled under paused policy, matching
current Patroni.

`POLICY` carries Patroni's active or paused state. `EVENTS` is a bounded
long-poll used for shutdown safepoints and command completion. Events have
monotonic sequence numbers and explicit acknowledgement.

## Commands

- `START`
- `STOP`
- `RESTART`
- `PROMOTE`
- `FOLLOW`
- `BOOTSTRAP`
- `CLONE`
- `REWIND`
- `CRASH_RECOVERY`
- `POST_BOOTSTRAP`
- `REINITIALIZE`
- `APPLY_CONFIG`
- `CALLBACK`
- `REMOVE_DATA`
- `MOVE_DATA`
- `SET_BOOTSTRAP`
- `RESET_RECOVERY`
- `CHECK_DIVERGENCE`
- `APPLY_SYNC`
- `APPLY_SLOTS`
- `COPY_SLOTS`
- `CHECKPOINT`
- `ARCHIVE_WAL`
- `FENCE`

New command parameters use enums instead of booleans. Targets contain public
host and port data. The agent adds locally held credentials.

## Command states

- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `CANCELLED`
- `FENCED`

Long operations return a command ID immediately. The controller polls status.
Logs remain local to the agent. Results contain only bounded domain data.

Shutdown commands may emit `SAFEPOINT` with checkpoint and previous WAL
locations. This replaces the current in-process callback without changing DCS
release ordering.

## Snapshot

`NodeSnapshot` contains:

- Agent boot ID and sequence.
- Observed and desired PostgreSQL roles.
- PostgreSQL state, system identifier, and server version.
- Timeline, WAL positions, and start time.
- Replication, synchronous, and slot summaries.
- Pending restart reason.
- Active command and phase.
- Watchdog health.
- Configuration revision and hash.
- Structured observation failures.

It excludes credentials, credential-bearing DSNs, raw configuration,
environment variables, and command output.

Status requests carry a freshness enum. REST health, readiness, status, and
metrics use the same fresh/retry behaviour as current Patroni. HA-cycle reads
may reuse only the same values current Patroni already caches within a cycle.
