# Patroni Jepsen gate

The default Compose topology runs three split Patroni members and three etcd
members. The mixed overlay replaces `patroni1` with monolithic Patroni.

```sh
docker compose build
docker compose up -d
docker exec jepsen-test bash rollout.sh
docker exec jepsen-test bash run.sh
docker compose down
```

Use `docker-compose-mixed.yml` as a second Compose file for the mixed topology.

The required CI job runs recorded seeds `17`, `29`, and `43`. Split runs cover
PostgreSQL 13 and 18; the mixed run covers PostgreSQL 17. Each 15-minute cycle
combines two of:

- controller, agent, or PostgreSQL kill/pause;
- socket loss or whole-member restart;
- etcd or Patroni network partition;
- switchover.

The checker rejects lost committed writes, unexpected writes, and sampled
overlapping writable primaries. Each primary probe holds a one-second write
transaction on every member concurrently. CI retains the history, nemesis
events, checker output, DCS state, Patroni logs, PostgreSQL logs, and final
cluster state for 14 days.

`rollout.sh` first converts one replica from split mode to monolithic Patroni
and back. It proves PostgreSQL stops between managers and PGDATA keeps the same
system identifier.

Set `JEPSEN_SEED`, `JEPSEN_TIME_LIMIT`, `JEPSEN_FINAL_TIME_LIMIT`,
`JEPSEN_RECOVERY_SECONDS`, or `JEPSEN_RUN_TIMEOUT` to reproduce a run. The
repository ruleset must require all `Jepsen tests / jepsen` matrix jobs.

## Performance qualification

`docker-compose-monolith.yml` makes all three Patroni members monolithic.
`docker-compose-performance.yml` enables debug logs for HA-cycle extraction.
Run each pure topology with PostgreSQL 18, then set `ttl=20`, `loop_wait=1`,
and `retry_timeout=3` through `patronictl edit-config`.

`docker exec jepsen-test bash performance.sh` runs three warmups and 30 measured
switchovers and failovers. It waits for one leader and two streaming quorum
standbys between samples. `ha-cycle.awk` extracts milliseconds from each debug
`Lock owner` line through its HA result line.
