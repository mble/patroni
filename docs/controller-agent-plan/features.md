# Split-mode feature status

| Feature | Status |
|---|---|
| Linux, Kubernetes, PostgreSQL, etcd3 | Supported |
| Existing REST and DCS formats | Compatible |
| Mixed monolithic and split members | Supported |
| Bootstrap, clone, rewind, reinitialize | Supported |
| Synchronous replication and slots | Supported |
| Watchdog and failsafe mode | Supported |
| Pause and scheduled restart | Supported |
| Same-Pod Unix socket | Supported |
| Different protocol minor versions | Unsupported; fail closed |
| Other DCS implementations | Unqualified |
| Citus/MPP | Unsupported |
| Windows | Unsupported |
| Remote agent | Unsupported |
| Bare-metal agent supervision | Unsupported |
| PostgreSQL survival across agent-container restart | Unsupported |

Monolithic Patroni remains supported. Removal requires a separate decision and:

- two supported release cycles of split-mode qualification;
- parity for every supported DCS, PostgreSQL platform, and extension mode;
- retained fault-soak evidence without a semantic regression;
- a tested recovery path that does not depend on monolithic Patroni.
