# ADR 0001: Split controller and agent

Status: accepted, 2026-09-02

## Context

Patroni currently combines DCS policy and local PostgreSQL control. Kubernetes
deployments therefore give one process both DCS and PostgreSQL credentials,
PGDATA, the watchdog, and the PostgreSQL process tree.

We need separate credentials, mounts, and failure domains without changing
Patroni's cluster semantics.

## Decision

Add an optional same-Pod split mode:

```text
controller ── AF_UNIX ── agent ── PostgreSQL
    │                     │
    └── etcd3             ├── PGDATA
                          └── watchdog
```

The controller owns HA policy, DCS access, peer requests, dynamic
configuration, and public REST. The agent owns PostgreSQL observation and
mutation, credentials, PGDATA, callbacks, replica creation, rewind, watchdog,
and process management.

The boundary exposes domain observations and typed commands. It never exposes
raw SQL, shell commands, credentials, process handles, callables, or Patroni
objects.

Existing monolithic behavior is normative. The split changes placement and
trust boundaries only. A semantic difference needs its own approved ADR.

## Safety

- The controller remains the sole election authority.
- Active primary-producing commands require current `LEADER`, `INITIALIZER`,
  or `FAILSAFE` authority.
- Authority expiry uses the current offline-demotion path no later than current
  DCS and watchdog deadlines.
- Paused mode retains current manual-control and watchdog behavior. Authority
  expiry alone does not demote.
- Agent loss stops DCS renewal. The controller never deletes the leader key
  without proof that PostgreSQL stopped or fenced.
- Shutdown releases the leader key only at the same current safepoint, or after
  PostgreSQL stops.
- PostgreSQL retains helper orphaning and PID-file adoption. The agent
  container's PID 1 adopts and reaps the process tree.
- Grants are monotonic, bounded, and memory-only. Commands are idempotent and
  bounded.

## Compatibility

Initial qualification covers Linux, Kubernetes, PostgreSQL, same-Pod Unix
sockets, and `etcd3`. Monolithic and split members may coexist.

Citus/MPP, Windows, remote agents, bare-metal supervision, etcd v2, and other
DCS qualification remain outside the first release. Monolithic Patroni remains
the rollback path.

## Consequences

The controller cannot read PGDATA or PostgreSQL credentials. The agent cannot
authenticate to the DCS. A compromised controller remains trusted for HA
policy; this design does not tolerate a Byzantine controller.

The Unix protocol adds a versioned compatibility surface and latency within
existing deadlines. Differential traces and Jepsen are release gates.

## Rejected

- A remote agent adds network partitions and identity work without improving
  the initial same-Pod isolation goal.
- Shared PostgreSQL and DCS credentials preserve the current blast radius.
- Agent-side election duplicates HA policy and creates a second consensus
  system.
- Respawning only the agent daemon can attach fresh authority to an uncertain
  postmaster. The initial container restarts as one unit instead.
