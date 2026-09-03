# Architecture divergences

## Summary

Current Patroni is one process with shared access to the DCS, PostgreSQL,
PGDATA, watchdog, REST API, and every credential. The divergence splits it into
a policy controller and a local PostgreSQL agent.

The intended divergence is structural. Existing Patroni behaviour is the
compatibility specification. Split mode preserves HA decisions, DCS ordering,
timeouts, REST responses, callbacks, bootstrap recovery, pause, watchdog, and
shutdown behaviour.

| Concern | Current Patroni | Split Patroni |
|---|---|---|
| HA policy | Patroni process | Controller |
| DCS and credentials | Patroni process | Controller only |
| Public REST API | Patroni process | Controller |
| PostgreSQL SQL and credentials | Patroni process | Agent only |
| PGDATA and configuration files | Patroni process | Agent only |
| PostgreSQL process tree | Orphaned and adopted by init | Same semantics in agent container |
| Bootstrap and rewind | Called directly by HA | Typed agent commands |
| Slots and synchronous state | Shared internal objects | Controller plans; agent applies |
| Watchdog | Driven directly by HA loop | Driven by equivalent controller policy over IPC |
| Internal communication | Python calls and objects | Versioned Unix-socket protocol |
| Primary safety | HA loop, DCS timing, failsafe, pause, watchdog | Same policy enforced across IPC |

## Application structure

Current startup constructs DCS, watchdog, PostgreSQL, REST, and HA objects in a
single `Patroni` instance. `Ha` reads and mutates `Postgresql` and its nested
bootstrap, rewind, configuration, slot, sync, callback, and cancellation
objects. REST status also queries PostgreSQL directly.

The split replaces those dependencies with one boundary:

```text
controller HA
    │
NodeControl
    │
AgentClient ── AF_UNIX ── AgentService
                              │
                       PostgresManager
                              │
                    existing local drivers
```

The controller receives coherent snapshots and submits high-level commands. It
cannot access PostgreSQL connections, PGDATA, process handles, or nested agent
objects.

## Process model

Current postmaster startup launches an intermediate process and deliberately
orphans PostgreSQL to init. Patroni later manages it through its PID file and
`psutil`.

Split mode retains that startup and adoption model. The agent daemon remains the
sole PostgreSQL lifecycle authority and continues to manage postmaster through
its PID file and `psutil`. An independent monitor fences any primary lacking
current authority.

The agent container's PID 1 supervisor adopts and reaps PostgreSQL. It watches
the agent daemon. Graceful shutdown invokes current active or paused Patroni
behaviour. Unexpected daemon exit causes the supervisor to exit without
respawning it; the container runtime then terminates remaining processes. This
preserves postmaster discovery and adoption without promising PostgreSQL
survival across an agent-container restart.

Bare-metal Patroni can restart and adopt a surviving postmaster under systemd,
with safety relying on DCS TTL and watchdog. Equivalent split-mode bare-metal
recovery is outside the initial Kubernetes scope.

Monolithic startup remains unchanged while split mode reaches parity.

## Primary authority transport

The controller remains responsible for election. The agent does not read the
DCS. Instead, the controller sends bounded authority grants after proving one
of these conditions:

- It acquired or renewed the leader lock.
- It acquired the initialization lock.
- It completed a valid failsafe topology check.

In active mode, the agent rejects primary-producing commands without a matching
grant. Grant expiry runs Patroni's existing offline-demotion behaviour. Grants
use monotonic deadlines derived from the existing DCS and watchdog timing rules
and are never persisted.

Paused mode is carried as explicit policy rather than an expiring grant. It
preserves Patroni's current manual-control, DCS-failure, shutdown, and disabled
watchdog behaviour.

The grant is an IPC representation of an existing Patroni decision, not a new
lease or election rule.

## Behaviour preservation across new failures

### Controller loss

Current Patroni and PostgreSQL usually fail together. In split mode the agent
may remain alive after controller failure. In active mode it runs the same
offline-demotion path Patroni uses when DCS renewal becomes impossible. In
paused mode it preserves PostgreSQL state, matching current Patroni.

### Agent loss

The controller stops renewing the leader key. It does not delete the key merely
because the socket failed: it cannot prove PostgreSQL stopped. Other nodes wait
for DCS expiry unless the agent acknowledged fencing.

### Demotion

Current demotion can invoke in-process callbacks at PostgreSQL shutdown
safepoints and release the leader key before postmaster fully exits. The agent
emits an equivalent typed safepoint event. The controller performs the same
replica checks and DCS update. Event loss falls back to full-stop handling and
never releases earlier.

### Paused mode

Current shutdown may leave PostgreSQL running as primary while paused. Split
mode preserves that behaviour. The agent records the last in-memory paused
policy, disables watchdog handling as Patroni does, and does not demote solely
because the controller or DCS became unavailable.

### Bootstrap

Initial bootstrap becomes an explicit `INITIALIZER` authority workflow because
PostgreSQL becomes primary before the leader key exists. The controller and
agent reproduce current `/initialize`, PGDATA, failure cleanup, and restart
recovery outcomes. Any command journal provides idempotency only; it does not
introduce a new automatic recovery outcome.

### Failsafe mode

The controller still contacts every known member. A successful check produces a
bounded `FAILSAFE` grant. Failure or expiry runs the same demotion path as
current Patroni.

### Watchdog

The agent preserves current activation, timeout, keepalive, reload, demotion,
pause, and required-watchdog behaviour. The controller transports the existing
TTL, loop, retry, and safety-margin values.

## Configuration and secrets

Current deployments use one configuration and environment. Split mode uses two
validated configurations.

Controller configuration contains:

- Node identity and public addresses.
- DCS settings and credentials.
- HA policy and timings.
- REST API and peer credentials.
- Agent socket client settings.

Agent configuration contains:

- PostgreSQL binaries and PGDATA.
- PostgreSQL authentication.
- Bootstrap and replica methods.
- Callbacks, rewind, and watchdog.
- Agent socket server settings.

The controller sends filtered, versioned dynamic PostgreSQL plans. The agent
never returns credentials or raw configuration. Plans may contain documented
callback or replica-method strings already present in Patroni configuration;
they cannot request an ad hoc command.

Callbacks retain their documented action, role, and scope arguments. Custom
replica methods retain documented arguments and configuration. They run in the
agent environment and therefore cannot depend on controller-only DCS secrets or
undocumented inherited variables.

## Protocol boundary

The local protocol uses a bounded, versioned Unix-socket request/response
format. Messages contain DTOs and enums only. It forbids serialized Patroni
objects, callables, arbitrary SQL, arbitrary shell commands, and `pickle`.

Long operations are asynchronous. The agent logs detailed output locally and
returns bounded progress and results. Fence requests preempt active work.

## REST and DCS compatibility

The controller preserves existing public REST responses and DCS records. This
allows monolithic and split members in one cluster during rollout.

REST PostgreSQL fields come from agent observations with the same freshness and
retry behaviour as current handlers. Controller-owned DCS fields are added
before responding. Mutating REST endpoints submit typed agent commands rather
than calling PostgreSQL objects.

## Kubernetes deployment

Split mode uses two containers in one Pod:

- The controller uses a distroless role image without PostgreSQL or psycopg.
- The agent uses an official PostgreSQL role image without DCS client packages.
- A shared `emptyDir` contains only the control socket.
- Only the agent mounts PGDATA and PostgreSQL secrets.
- Only the controller mounts etcd credentials.
- PostgreSQL runs in the agent container.
- Public REST runs in the controller container.
- PostgreSQL and REST retain their existing Pod network addresses.
- Distinct UIDs, peer credential checks, and one shared socket GID bind roles.
- Read-only roots, dropped capabilities, and runtime seccomp constrain both.

Both containers share the Pod network namespace. Standard Kubernetes
NetworkPolicy cannot isolate their egress separately. Credential and mount
separation remain effective; strict network separation requires a different
topology or identity-aware networking.

## Compatibility limits

Initial split support is Linux, Kubernetes, PostgreSQL, and `etcd3`. The code
does not remove other DCS implementations, but they remain unqualified in split
mode. Citus/MPP, Windows, bare-metal supervision, remote agents, and etcd v2 are
excluded initially.

Monolithic Patroni remains the rollback path. Removing it is a later design
decision, not part of this divergence.
