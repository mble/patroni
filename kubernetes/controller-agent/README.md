# Controller-agent StatefulSet

This example runs one controller and one agent per Pod with etcd3. Build the
role images from the repository root:

```console
docker build -f kubernetes/controller-agent/Dockerfile.controller \
  -t patroni-controller:latest .
docker build -f kubernetes/controller-agent/Dockerfile.agent \
  -t patroni-agent:latest .
scripts/integration/check-role-images.sh
scripts/integration/scan-role-images.sh
```

Both builds install the current checkout from hash-locked role manifests. The
controller is distroless. The agent retains the official PostgreSQL runtime,
shell, callbacks, and local lifecycle tools. The scan writes SBOMs and
vulnerability reports to `role-image-artifacts` and requires Trivy and `jq`.

Create `patroni-split-postgres` with `superuser-password` and
`replication-password`. Create `patroni-etcd-client` with `ca.crt`, `tls.crt`,
and `tls.key`. Change the etcd endpoint and storage class, then run
`kubectl apply -k kubernetes/controller-agent`.

Only the agent receives PostgreSQL secrets and mounts PGDATA. Only the
controller mounts etcd credentials. `/run/patroni` is the sole shared volume.
The agent runs as UID/GID 999. The controller runs as UID/GID 65532 and receives
supplemental GID 999 for the socket. Neither gets capabilities or a writable
root. A bounded init container makes `/run/patroni` root:999 mode 0770. The
agent accepts that directory without weakening socket ownership checks.
PostgreSQL uses the `pgdata` subdirectory because the PVC mount root is not
owned by UID 999.

The 60-second termination grace exceeds the configured 30-second DCS TTL plus
the 10-second primary stop bound. Recalculate it when either value changes.

Containers in a Pod share one network namespace. Kubernetes NetworkPolicy
cannot allow etcd egress for the controller while denying it to the agent.
Credential separation prevents authenticated agent access. Stronger network
isolation needs separate Pods or an identity-aware proxy.

The headless Service exposes every member. Route writes using Patroni REST or a
role-aware proxy; etcd DCS mode does not maintain Kubernetes role labels.
