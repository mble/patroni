# M10: Parity, rollout, and rollback

Status: pending performance acceptance

## Goal

Make split mode operable without forcing a flag-day cluster conversion.

## Work

1. Publish configuration and operations documentation.
2. Publish supported and excluded feature matrices.
3. Preserve PGDATA, REST, and DCS compatibility.
4. Add upgrade and rollback procedures.
5. Keep monolithic Patroni available.
6. Publish the completed semantic-parity matrix.
7. Define later criteria for removing monolithic mode.

## Rollout

1. Replace replicas with split Pods.
2. Wait for healthy replication and agent metrics.
3. Switchover to a split replica.
4. Replace the former primary.

Rollback starts monolithic Patroni only after the agent confirms fencing.

## Tests

- Full unit, lint, type, and docs suites.
- Split etcd3 behaviour suite on representative oldest and newest PostgreSQL.
- Mixed-member upgrade and rollback.
- Controller/agent minor-version skew.
- Required extended Jepsen fault soak with retained histories.

## Reviews

### Correctness

Complete semantic-parity, feature-parity, and migration trace reviews. Rollback
cannot overlap two local managers of one PGDATA.

### Security

Complete the threat-model diff, secret audit, manifest audit, and image audit.

### Performance

Publish baseline comparisons. Any semantic regression blocks release; any
accepted performance regression requires explicit approval.

## Exit

Split mode is deployable, the release Jepsen soak passes, and no semantic
difference remains. Removing monolithic mode is a separate decision.

## Result

Configuration, feature status, rollout, rollback, and qualification evidence
are published under `docs/controller-agent-plan`. Monolithic Patroni remains
available. Final-revision PostgreSQL 13 split, PostgreSQL 18 split, and
PostgreSQL 17 mixed campaigns pass locally with same-PGDATA rollout. The matrix
is a required merge gate. Wire-version skew fails closed. M10 awaits acceptance
of the measured process and REST overhead.
