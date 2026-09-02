# Security boundary

## Trust

| Component | Trusted for | Must not receive |
|---|---|---|
| Controller | HA policy, etcd3 writes, peer and REST authentication | PGDATA, PostgreSQL credentials, watchdog, process handles |
| Agent | PostgreSQL lifecycle, local SQL, PGDATA, watchdog, callbacks | DCS credentials, DCS client, peer credentials |
| Unix transport | Framing, authentication, bounded delivery | Policy, SQL, shell, or object serialization logic |
| PostgreSQL | Database execution and storage | DCS credentials |
| Kubernetes runtime | Pod process and mount isolation | Application credentials beyond mounted targets |

The controller is trusted but not Byzantine-resistant. Compromise of either
process is contained by its credentials and mounts, not by shared Pod network
isolation.

## Kubernetes mounts

| Mount | Controller | Agent |
|---|---|---|
| etcd3 credentials | read-only | absent |
| REST and peer credentials | read-only | absent |
| PostgreSQL credentials | absent | read-only |
| PGDATA | absent | read-write |
| watchdog device | absent | read-write when configured |
| control socket `emptyDir` | read-write | read-write |

The socket directory is private to the Pod, has fixed ownership and mode, and
contains no other files. The agent rejects unexpected peer credentials,
versions, frame sizes, enums, sequence reuse, and expired deadlines.

## Excluded threats

- A malicious controller issuing valid but unsafe HA intent.
- A compromised Pod runtime, node root, kernel, or PostgreSQL superuser.
- Container-specific egress filtering through standard NetworkPolicy; Pod
  containers share a network namespace.
- Remote-agent authentication and network partitions.

These exclusions do not permit secrets, raw SQL, raw shell commands, arbitrary
paths, or serialized Python objects on the protocol.
