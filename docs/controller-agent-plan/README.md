# Controller-agent divergence

Split Patroni into two same-Pod processes:

```text
controller ── AF_UNIX ── agent ── PostgreSQL process tree
    │                     │
    └── etcd3             └── PGDATA, SQL, watchdog
```

The controller owns HA policy, DCS access, peer communication, dynamic
configuration, and the public REST API. The agent owns PostgreSQL, PGDATA,
local credentials, callbacks, rewind, bootstrap, and process supervision.

The split changes deployment and trust boundaries, not Patroni behaviour.
Existing HA, DCS, REST, callback, bootstrap, pause, watchdog, and shutdown
semantics are authoritative. Any unavoidable difference requires a separate,
approved compatibility decision.

The accepted decision is [ADR 0001](adr/0001-controller-agent-split.md).
[M00 evidence](m00/baseline.md), the [parity matrix](m00/semantic-parity.md),
and the [trace contract](m00/trace-contract.md) freeze the starting point. See
the [feature matrix](features.md), [operations guide](operations.md), and
[qualification record](qualification.md).

## Invariants

1. Outside paused mode, PostgreSQL may be primary only under current leader,
   initializer, or failsafe authority.
2. Paused mode preserves Patroni's existing manual-control semantics, including
   its watchdog and DCS-failure behaviour.
3. An explicit fence command preempts every other command. Automatic authority
   expiry does not fence paused PostgreSQL.
4. The controller never deletes the leader key after agent loss without proof
   that PostgreSQL stopped. It lets the key expire.
5. Grants are never persisted.
6. No raw SQL, shell command, callback, credential, or Patroni object crosses
   the control socket.
7. Every queue, message, retry, wait, and history is bounded.

Authority sources are `LEADER`, `INITIALIZER`, and `FAILSAFE`. See
[divergences.md](divergences.md), [architecture.md](architecture.md), and
[protocol.md](protocol.md).

## Initial scope

- Linux and Kubernetes.
- PostgreSQL with `etcd3`.
- Same-Pod Unix socket transport.
- Mixed monolithic and split cluster members.
- Monolithic Patroni remains available until parity.

Initial exclusions: Citus/MPP, Windows, bare-metal supervision, remote agents,
etcd v2, and qualification of other DCS implementations.

## Milestones

| ID | Slice |
|---|---|
| [M00](milestones/00-baseline-adr.md) | Complete: baseline and ADR |
| [M01](milestones/01-safety-model.md) | Complete: models and safety state machine |
| [M02](milestones/02-observations.md) | Complete: coherent observations |
| [M03](milestones/03-lifecycle.md) | Complete: lifecycle commands |
| [M04](milestones/04-recovery-config.md) | Complete: recovery and configuration |
| [M05](milestones/05-replication-watchdog.md) | Complete: replication and watchdog |
| [M06](milestones/06-process-ownership.md) | Complete: process supervision and adoption |
| [M07](milestones/07-unix-transport.md) | Complete: Unix transport |
| [M08](milestones/08-controller.md) | Complete: controller mode and REST parity |
| [M09](milestones/09-kubernetes.md) | Complete: Kubernetes and fault qualification |
| [M10](milestones/10-rollout.md) | Complete: parity, rollout, and rollback |
| [M11](milestones/11-performance.md) | Complete: split-process performance pass |
| [M12](milestones/12-minimal-images.md) | Complete: minimal runtime images |
| [M13](milestones/13-review-hardening.md) | Local qualification complete; merge-revision CI pending |

Complete milestones in order. Each requires correctness, security, and
performance review before merge. Remediation repeats affected reviews.

## Branch

`controller-agent` was created from `5f2c94c8`.
