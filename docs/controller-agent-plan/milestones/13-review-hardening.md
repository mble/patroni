# M13: Review hardening

Status: local qualification complete; merge-revision CI pending

## Objective

Close the security, durability, availability, and correctness findings from
the post-M12 review.

## Work

1. [x] Bind primary-producing command kinds to safe target roles.
2. [x] Observe PostgreSQL independently and fence after controller loss.
3. [x] Reconcile executor and safety command state after failures and reconnects.
4. [x] Accept the safe Kubernetes `fsGroup` socket directory.
5. [x] Isolate HA RPC traffic from slow REST observations.
6. [x] Wake callback acknowledgement waits during cancellation.
7. [x] Fail liveness when the RPC accept loop exits.
8. [x] Wire command phases into execution.
9. [x] Fence PostgreSQL if graceful agent shutdown cannot finish.

## Exit

Each regression test fails before its fix and passes afterward. Unit, static,
manifest, integration, and fault-injection gates pass.

## Qualification

See the [M13 acceptance matrix](../m13-acceptance.md).

- 901 non-Unix and 19 isolated Unix tests pass. The macOS sandbox rejects
  late-suite Unix socket binds when all tests share one process.
- Flake8 and changed-production Pyright checks pass.
- Subprocess command, transport, cancellation, and fencing cases pass.
- The k3d reference deployment passes identity, permission, probe, authority
  expiry, and independent-restart faults.
- Final role images pass size, metadata, SBOM, and vulnerability gates.
- PostgreSQL 13 same-PGDATA split-to-monolith-to-split rollout passes.
- PostgreSQL 13 and 18 split Jepsen campaigns pass.
- PostgreSQL 17 mixed-version Jepsen and same-PGDATA rollout pass.
- The controller-loss smoke test is wired into Linux CI. macOS cannot run it
  because the production transport requires Linux `SO_PEERCRED`.
