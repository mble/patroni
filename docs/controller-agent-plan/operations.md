# Split-mode operations

## Configuration

The reference images provide fixed role entry points. Outside those images,
run `dumb-init --single-child -- patroni-agent agent.yml` as PID 1 in the
PostgreSQL container and `patroni-controller controller.yml` in the DCS
container. Both processes need the same absolute Unix-socket path, scope, name,
and Pod addresses. `patroni-agent-supervisor` remains a Python fallback.

The agent configuration owns PostgreSQL, PGDATA, PostgreSQL authentication,
bootstrap methods, callbacks, rewind, watchdog, and the `agent` section. It
must not contain DCS, REST, or controller sections.

The controller configuration owns etcd3, REST, peer access, DCS bootstrap
policy, public PostgreSQL metadata, and the `controller` section. It must not
contain PGDATA, PostgreSQL authentication, or local bootstrap settings.

Use the Kubernetes example in `kubernetes/controller-agent`. Keep the socket
volume private to the two containers. Do not mount either credential set into
the other container.

## Rollout

1. Back up DCS configuration and confirm every replica is streaming.
2. Replace one replica with the split Pod, preserving its name and PGDATA.
3. Confirm `/readiness`, replication, and `patroni_agent_*` metrics.
4. Repeat for other replicas.
5. Switchover to a split replica.
6. Replace the former primary.

Mixed monolithic and split members use the same REST and DCS records. Do not
run monolithic Patroni and the split agent against one PGDATA concurrently.

## Component upgrades

Protocol major and minor versions must match. Stop the controller, stop the
agent container, upgrade both images, then restart the Pod. Kubernetes may
start either container first; the controller waits for the socket.

Independent image rollout is limited to builds with the same wire version and
capabilities. Version mismatch fails closed. Qualify each supported skew before
declaring it supported.

## Rollback

1. Stop the controller so DCS renewal ends.
2. Stop the agent gracefully and confirm PostgreSQL stopped.
3. If graceful fencing cannot be proven, wait for the leader TTL and verify a
   different leader before proceeding.
4. Start monolithic Patroni with the same name, PGDATA, addresses, and config.
5. Confirm replication before rolling back another member.

Paused clusters retain current Patroni shutdown semantics and may leave
PostgreSQL running. Resume or stop PostgreSQL explicitly before rollback.

## Monolithic retirement

Monolithic mode remains the rollback path. Do not remove it until split mode
has two supported release cycles, complete platform parity, retained fault-soak
evidence, and an independent tested recovery path.

## Diagnostics

Use the controller REST API for cluster state. Use agent metrics for socket,
authority, command, fence, and configuration state. PostgreSQL and command
details remain in the agent logs. DCS errors remain in the controller logs.
