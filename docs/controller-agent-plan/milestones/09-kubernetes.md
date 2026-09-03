# M09: Kubernetes and fault qualification

Status: complete, 2026-09-03

## Goal

Prove the split under real Pod lifecycle and network failures.

## Work

1. Add a two-container StatefulSet example.
2. Share only the control-socket `emptyDir`.
3. Mount PGDATA and PostgreSQL secrets only in the agent.
4. Mount etcd credentials only in the controller.
5. Disable automatic service-account token mounting; mount explicit tokens only
   where required.
6. Add container-specific probes and resource limits.
7. Derive termination grace from PostgreSQL and DCS safety deadlines.
8. Add differential split behaviour tests.
9. Extend Jepsen with controller, agent, socket, etcd, peer, and Pod nemeses.
10. Make the defined Jepsen campaign a required M09 merge gate.
11. Document the shared network-namespace limitation.

## Fault tests

- Kill or suspend controller, agent, and PostgreSQL independently.
- Remove, replace, stall, and permission-block the socket.
- Partition controller from etcd.
- Partition controller from peers during failsafe.
- Restart containers in both orders.
- Evict and terminate the Pod.
- Roll versions independently within the supported skew.
- Assert no overlapping writable primaries.
- Compare monolithic and split client-visible histories and role/DCS traces.

## Required Jepsen campaign

- Run fixed and recorded random seeds.
- Kill and pause controller, agent, and PostgreSQL independently.
- Partition etcd and Patroni peers.
- Fail the Unix socket during commands and safepoints.
- Restart and evict Pods.
- Combine process and network faults.
- Retain histories, nemesis logs, DCS traces, and checker output.
- Reject overlapping writable primaries or histories outside Patroni's current
  accepted outcomes.

## Reviews

### Correctness

Active-mode fault traces show demotion or fencing before a competing promotion.
Paused-mode traces preserve current behaviour. Probe and termination ordering
do not weaken Patroni's safety model.

### Security

Inspect effective mounts, tokens, UIDs, groups, capabilities, socket ownership,
and read-only roots. Record that standard NetworkPolicy is Pod-wide.

### Performance

Compare failover, switchover, HA CPU, agent CPU, memory, and PostgreSQL workload
throughput with M00.

## Exit

The required Jepsen campaign and differential behaviour suite pass. Repeated
fault runs produce no split brain, semantic regression, or unbounded resource
use.

## Result

The two-container StatefulSet, socket-only shared volume, credential split,
probes, resource bounds, and termination budget are in
`kubernetes/controller-agent`. The socket trace test compares direct and split
observations. The required Jepsen matrix covers split and mixed clusters with
recorded seeds, combined process/network faults, write probes, and retained
evidence. Repository branch protection must require its matrix jobs.
