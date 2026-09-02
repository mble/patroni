# Architecture

## Ownership

| Controller | Agent |
|---|---|
| DCS credentials and client | PostgreSQL credentials and connections |
| Leader election and HA policy | PostgreSQL process tree |
| Peer Patroni requests | PGDATA and configuration files |
| Public REST API | Bootstrap, clone, rewind, reinitialize |
| Dynamic configuration | Callbacks, slots, sync mechanics |
| Scheduled intent | Watchdog and local fencing |

The controller sends desired state. The agent returns observations and typed
command results. Neither reaches through the other layer.

## Semantic parity

Existing Patroni is the behavioural specification. Split mode must preserve:

- HA decisions and DCS mutation ordering.
- DCS TTL, retry, failsafe, and watchdog timing rules.
- Pause and manual-control behaviour.
- Bootstrap failure and restart recovery outcomes.
- Shutdown safepoints and leader-key release conditions.
- REST status freshness, status codes, and response bodies.
- Callback arguments, ordering, and cancellation behaviour.
- Replica-method arguments and documented configuration.

IPC may add latency only within existing deadlines. It must not add a new HA
policy. Differential tests compare monolithic and split traces.

```text
HA policy
   │
NodeControl
   │
AgentClient
   │
Unix codec
   │
AgentService
   │
PostgresManager
   │
Existing PostgreSQL drivers
```

Do not serialize `Cluster`, `Member`, `Patroni`, `Postgresql`, callables, or
process handles. Translate them into small domain records at the boundary.

## Authority transport

Authority grants carry the controller's existing Patroni decision across IPC;
they do not define a new lease policy. In active mode the agent accepts
primary-producing operations only with:

- `LEADER`: successful leader acquisition or renewal.
- `INITIALIZER`: successful creation of `/initialize`.
- `FAILSAFE`: successful contact with every failsafe member.

The grant deadline is derived from Patroni's adjusted `ttl`, `loop_wait`,
`retry_timeout`, last successful authority operation, and watchdog timing. It
must cause the same action no later than the existing HA loop would. Transport
deadlines consume the existing retry budget; they do not extend the DCS lease
horizon. Grant renewal runs independently from long PostgreSQL operations.

Paused mode is an explicit controller policy state, not an expiring authority
grant. The agent preserves PostgreSQL state, disables watchdog behaviour as
Patroni does today, and does not autonomously demote on controller or DCS loss.

## Agent state

```text
IDLE ── submit ──> BUSY ── result ──> IDLE
  │                  │
  └──── fence ───────┴────> FENCING ──> IDLE
```

In active mode, any observed primary without authority enters `FENCING`.
Fencing follows Patroni's existing offline-demotion path and deadlines. Paused
mode does not enter automatic fencing solely because authority expired.

Only one mutating command may run. Repeated command IDs return the original
status. Conflicting reuse is rejected.

## Failure rules

| Failure | Rule |
|---|---|
| Controller exit, active | Run existing offline-demotion behaviour before DCS expiry. |
| Controller exit, paused | Preserve PostgreSQL and disabled-watchdog semantics. |
| Agent exit | PostgreSQL exits with its container; stop DCS renewal. |
| Socket loss | Apply both rules above. |
| Agent unreachable | Do not delete leader key without fencing proof. |
| DCS loss | Grant only after valid failsafe consensus. |
| Agent daemon exit | PID 1 exits; the runtime terminates PostgreSQL and restarts the container. |
| Controller restart | Re-establish authority before granting. |
| Promotion loses authority | Cancel, then fence if promotion completed. |
| Bootstrap interruption | Reproduce Patroni's existing `/initialize` and PGDATA recovery. |

Agent restart in the initial Kubernetes deployment restarts the container and
PostgreSQL. Bare-metal postmaster survival is outside the initial scope.

## Shutdown events

The agent reports typed shutdown safepoint events containing the same checkpoint
and previous WAL locations used by current callbacks. The controller performs
the existing replica checks and may release the leader key at the same logical
point. Lost events fall back to current full-stop handling; they never cause an
earlier release.

## Process ownership

Split mode retains Patroni's postmaster orphan and PID-file adoption semantics.
The agent daemon launches PostgreSQL through the existing helper. PostgreSQL is
reparented to the agent container's PID 1 supervisor, which reaps children.

The agent daemon remains the sole lifecycle authority. It discovers and manages
PostgreSQL through the existing PID-file and `psutil` mechanisms. This preserves
postmaster discovery and adoption behaviour.

The PID 1 supervisor watches the agent daemon. Graceful shutdown invokes current
active or paused Patroni shutdown behaviour. If the daemon exits unexpectedly,
the supervisor exits rather than respawning it. The container runtime terminates
the remaining container processes, and Kubernetes restarts the container.

Split mode does not promise that PostgreSQL survives an agent-container restart.
Providing Patroni-style bare-metal crash adoption requires watchdog-backed
authority recovery and remains outside the first release.

The supported Kubernetes deployment relies on the container runtime terminating
remaining processes when its main process exits. Bare-metal crash supervision
is outside the first release.

## Kubernetes boundary

Only the agent mounts PGDATA and PostgreSQL secrets. Only the controller mounts
etcd credentials. Both mount the control-socket `emptyDir`.

Containers in one Pod share a network namespace. Standard NetworkPolicy cannot
prevent only the agent from reaching etcd. Credential separation still blocks
authenticated DCS access. Strong network isolation requires a different
deployment or identity-aware networking.
