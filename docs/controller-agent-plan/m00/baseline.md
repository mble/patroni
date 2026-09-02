# M00 baseline

Date: 2026-09-02

Product base: `5f2c94c82a9cbea388c40451bdc8444683bae367`

Evidence commit: `05698cafc81134b4b1768941532d62d0ee60019a`

No product code differs between these revisions.

## Environment

| Item | Value |
|---|---|
| Host | Darwin 24.6.0, arm64 |
| Local Python | 3.9.6 |
| PostgreSQL | 18.6, Postgres.app |
| Docker | 29.4.0 |
| Integration image | `patroni-dev:18`, PostgreSQL 18, Python 3.13 |
| Integration DCS | etcd 3.3.13 through the `etcd3` client |

Python 3.14.7 cannot build the pinned macOS `psycopg2-binary==2.9.9`.
Python 3.9 is supported by this revision and supplies a binary wheel.

## Results

| Check | Result |
|---|---|
| Unit, excluding Raft files | 739 passed, 1 skipped, 6.55 s |
| Raft prefix | 4 passed, then hung in `pysyncobj` transport |
| Lint | passed |
| Pyright | 0 errors, 0 warnings |
| Sphinx HTML, `-W` | passed with network access |
| `basic_replication.feature`, etcd3 | 8 scenarios, 85 steps passed |
| `patroni_api.feature`, etcd3 | 7 scenarios, 111 steps passed |

The full unit command reached 564 passes, then hung. Isolated runs hang while
constructing `KVStoreTTL` in `tests/test_raft.py` with `pysyncobj==0.3.17`.
Raft is outside initial split qualification. The failure is a recorded base
condition, not an accepted split regression.

## Commands

```text
/private/tmp/patroni-m00-py39/bin/pytest \
  -p no:cacheprovider --doctest-modules --capture=fd -q \
  --ignore tests/test_raft.py --ignore tests/test_raft_controller.py \
  tests patroni

/private/tmp/patroni-m00-py39/bin/flake8 patroni tests setup.py

/private/tmp/patroni-m00-py39/bin/pyright \
  --pythonpath /private/tmp/patroni-m00-py39/bin/python patroni

/private/tmp/patroni-m00-py39/bin/sphinx-build -q \
  -d /private/tmp/patroni-m00-docs-doctree \
  docs /private/tmp/patroni-m00-docs-out \
  -b html -T -E -W --keep-going

docker build . --tag patroni-dev:18 --build-arg PG_MAJOR=18 \
  --file features/Dockerfile

docker run --volume REPOSITORY:/src --env DCS=etcd3 \
  --hostname RUN_NAME --name RUN_NAME --rm patroni-dev:18 \
  tox run -e py313-behave-etcd3-lin -- FEATURE
```

`FEATURE` was each of `features/basic_replication.feature` and
`features/patroni_api.feature`.

## Behavior samples

The harness uses a one-second `loop_wait`.

| Observation | Sample |
|---|---|
| HA cycle, lock observation to result log | n=40; p50 86 ms; p95 151 ms; max 574 ms |
| REST GET step | n=16; p50 58.5 ms; p95 67.8 ms |
| Paused API switchover request | 2.118 s; target primary 1.003 s later |
| Scheduled switchover | target leader after 11.067 s for a 10 s schedule |
| Failover after resume | target primary after 19.074 s |
| Initial bootstrap | process start 3.052 s; leader check immediate afterward |

These are qualification samples, not service-level objectives. Comparison uses
matched monolithic and split runs on the same host and image.
