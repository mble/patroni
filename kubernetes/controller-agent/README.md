# Controller-agent StatefulSet

This example runs one controller and one agent per Pod with etcd3. Build an
image containing this branch and `dumb-init` as
`patroni-controller-agent:latest`.

Create `patroni-split-postgres` with `superuser-password` and
`replication-password`. Create `patroni-etcd-client` with `ca.crt`, `tls.crt`,
and `tls.key`. Change the etcd endpoint and storage class, then run
`kubectl apply -k kubernetes/controller-agent`.

Only the agent receives PostgreSQL secrets and mounts PGDATA. Only the
controller mounts etcd credentials. `/run/patroni` is the sole shared volume.
Both containers run as UID/GID 999 without capabilities or writable roots.

The 60-second termination grace exceeds the configured 30-second DCS TTL plus
the 10-second primary stop bound. Recalculate it when either value changes.

Containers in a Pod share one network namespace. Kubernetes NetworkPolicy
cannot allow etcd egress for the controller while denying it to the agent.
Credential separation prevents authenticated agent access. Stronger network
isolation needs separate Pods or an identity-aware proxy.

The headless Service exposes every member. Route writes using Patroni REST or a
role-aware proxy; etcd DCS mode does not maintain Kubernetes role labels.
