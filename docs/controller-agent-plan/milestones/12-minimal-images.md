# M12: Minimal runtime images

Status: complete, 2026-09-03

## Goal

Make image contents match the controller-agent trust boundary without changing
Patroni semantics.

```text
controller image                    agent image
┌──────────────────────────┐        ┌──────────────────────────┐
│ controller policy + REST │        │ agent lifecycle          │
│ etcd3 client + CA bundle │◀─UDS──▶│ PostgreSQL + local tools │
│ no PostgreSQL toolchain  │        │ no DCS clients           │
└──────────────────────────┘        └──────────────────────────┘
```

## Baseline

Before M12, the reference used one 574 MiB image for both containers. The
measured image contains 207 OS packages and 29 Python distributions. Each
container can execute code owned by the other role.

The generic Kubernetes Dockerfile also installs upstream Patroni with the
`kubernetes` extra. The split reference must instead build this checkout with
only its role-specific dependencies and etcd3 support.

## Work

### Package boundaries

1. Define explicit, hash-locked controller and agent dependency manifests.
2. Omit psycopg from the controller and DCS client packages from the agent.
3. Retain `psutil` in the controller because current Patroni package imports
   require it. Refactoring those imports risks changing public module and
   monkey-patch semantics for negligible image savings.
4. Reject unexpected installed distributions in the image gate.
5. Retain the agent shell for callbacks and custom bootstrap parity.

### Images

1. Produce separate, multi-stage controller and agent images.
2. Build wheels from the checked-out revision with locked hashes.
3. Copy only runtime files into the final stages.
4. Remove pip, build tools, package indexes, editors, Git, `curl`, `jq`, unused
   DCS clients, Raft, `patronictl`, and unrelated console entry points.
5. Keep only the controller CA bundle and etcd3 client.
6. Keep only PostgreSQL, psycopg, psutil, required local utilities, and
   `dumb-init` in the agent.
7. Pin base images by digest and publish an SBOM for each image.

The first agent image remains based on the supported official PostgreSQL image.
A custom PostgreSQL runtime is a later optimization because it expands upgrade
and semantic-parity risk.

### Runtime boundary

1. Run the roles under distinct UIDs with one shared socket GID.
2. Configure exact peer UIDs and socket mode `0660`.
3. Retain read-only roots, dropped capabilities, disabled privilege escalation,
   bounded writable volumes, and disabled service-account tokens.
4. Add the runtime-default seccomp profile.
5. Reduce agent socket workers to two: one active connection and one reconnect.
6. Expose only PostgreSQL from the agent and REST from the controller.
7. Document that Pod-wide networking cannot enforce per-container egress.

## Budgets

- Controller image: at most 150 MiB uncompressed.
- Agent overhead: at most 75 MiB above its pinned PostgreSQL base image.
- No PostgreSQL binaries, psycopg, shell, or package manager in the controller.
- No DCS client, DCS credentials, REST listener, or Patroni CLI in the agent.
- No unapproved runtime executable or Python distribution in either SBOM.
- No fixable high or critical vulnerability introduced above the base images.

Budgets are release gates, not semantic trade-offs. A required dependency may
change a budget only through an explicit review with updated measurements.

## Tests

- Build and start each image without the other role's dependencies installed.
- Assert runtime user, UID, GID, mounts, capabilities, seccomp, entry point,
  exposed port, executable allowlist, and read-only root.
- Verify signal forwarding, child reaping, PID-file adoption, callbacks,
  bootstrap, rewind, and graceful active and paused shutdown.
- Run split and mixed same-PGDATA rollout tests.
- Run the full supported PostgreSQL and Jepsen fault matrix.
- Retain image inventories, SBOMs, scan results, sizes, and Jepsen histories.

## Exit

Both images meet their budgets and isolation assertions. Semantic parity,
rollout, rollback, and the required Jepsen matrix pass. The shared image is no
longer used by the reference deployment.

## Result

The reference uses separate digest-pinned, multi-stage images with eight
controller and nine agent Python distributions.

| Platform | Controller | Agent | PostgreSQL base | Agent overhead |
|---|---:|---:|---:|---:|
| amd64 | 57.6 MiB | 478.4 MiB | 420.0 MiB | 58.4 MiB |
| arm64 | 69.6 MiB | 505.1 MiB | 440.2 MiB | 64.9 MiB |

The controller has no shell, package manager, PostgreSQL binary, psycopg, or
Patroni console scripts. The agent has no installed DCS client or Patroni
console scripts. Image entry points expose only the assigned role. Kubernetes
uses distinct UIDs, peer credential checks, socket mode `0660`, two socket
workers, and `RuntimeDefault` seccomp.

CI enforces sizes, runtime metadata, exact Python distribution allowlists,
read-only startup, SBOM generation, and vulnerability deltas. The 2026-09-03
scan found no fixable high or critical vulnerability added above either pinned
base. Base findings remain base-image refresh work.

The 893-test suite, prior full PostgreSQL/Jepsen matrix, rollout tests, and the
post-M11 Jepsen regression cover unchanged Patroni semantics. M12 changes
packaging and the reference manifest, not policy or PostgreSQL lifecycle code.
