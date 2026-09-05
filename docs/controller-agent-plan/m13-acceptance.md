# M13 acceptance

M13 is complete for fork revision `ddeadb92`. No upstream merge is planned.

| Gate | Scenario | Required result | Automation |
|---|---|---|---|
| A01 | Block `STATUS` SQL while requesting `BASIC` state | HA reads and authority fencing remain responsive | Unit and process pass |
| A02 | Stop the controller on a writable primary | Agent stops PostgreSQL after grant expiry | k3d passes; Linux smoke is in CI |
| A03 | Restart the controller during each command phase | Completion reconciles; the next command runs without agent restart | 16 process cases pass |
| A04 | Deploy the reference StatefulSet on kind or k3d | UID, GID, mode, probes, and independent restarts pass | k3d passes |
| A05 | Saturate status RPC and reset both connections | Grants renew; workers remain bounded; control sequence stays valid | Process test passes |
| A06 | Cancel and fence during callback acknowledgement | Wait exits promptly; phases remain ordered | Unit and process pass |
| A07 | Fail the Unix accept loop in Kubernetes | Socket disappears and kubelet restarts the agent | k3d passes |
| A08 | Repeat M11 performance measurements | Any CPU, PSS, REST, or HA regression is reviewed | Matched repeat passes |

## Fault matrix

Run `START`, `PROMOTE`, `RESTART`, and `BOOTSTRAP` with controller loss during
`PREPARING`, `MUTATING`, `FINALIZING`, and after local completion but before
controller polling. Active policy must reject writes after authority expiry.
Paused policy must retain its documented manual-control behavior.

## Release gates

1. Fork release-revision CI runs the Python 3.7–3.14 unit and Behave matrix.
2. Local controller and agent images pass size, metadata, SBOM, and
   vulnerability checks.
3. PostgreSQL 13 and 18 split Jepsen campaigns pass.
4. PostgreSQL 17 mixed-version Jepsen and same-PGDATA rollout pass.
5. Local histories, logs, probe events, and performance results are retained.

## Fork release CI

- [Tests](https://github.com/mble/patroni/actions/runs/33927724847) pass.
- [Role images](https://github.com/mble/patroni/actions/runs/33927724796) pass.

## Local release results

Jepsen seeds 17, 43, and 29 covered PostgreSQL 13 split, PostgreSQL 17 mixed,
and PostgreSQL 18 split respectively. Each ran 900 seconds of faults, 120
seconds of final reads, and 30 seconds of recovery. All histories were valid,
with no lost or unexpected writes and no writable-primary overlaps.

The final controller and agent images are 73,002,846 and 529,667,429 bytes.
The agent base is 461,604,234 bytes. The pinned k3d deployment passed identity,
mode, single-primary, authority-expiry fencing, and independent-restart checks.

## Local performance repeat

The [matched M11–M13 repeat](m13-performance.md) found no attributable PSS,
CPU, REST, HA-cycle, switchover, or failover regression. The earlier PSS and
REST increases were environmental.
