# Semantic parity matrix

Implementation status: complete, 2026-09-03. Release status remains gated by
M10 qualification and performance acceptance.

Each split assertion is mandatory. Current code and tests define details not
restated here.

| Concern | Current behavior | Split assertion | Evidence |
|---|---|---|---|
| Bootstrap | `/initialize` is acquired before primary bootstrap; failures clean DCS and PGDATA according to current recovery paths | Same DCS order, terminal state, cleanup, and restart recovery | `ha.py`, `bootstrap.py`, custom bootstrap features |
| Leader acquisition | Promotion follows successful election and eligibility checks | No primary-producing command before matching authority | `ha.py`; basic replication feature |
| Leader renewal | DCS update, member state, watchdog keepalive, and HA result share the current cycle | Same mutation order and deadline; IPC consumes the retry budget | `ha.py`, `watchdog/base.py` |
| DCS loss | Current retry, failsafe, demotion, and watchdog rules apply | Same outcome and no later fence | `dcs_failsafe_mode.feature`, Jepsen gate |
| Pause | PostgreSQL remains manually controlled; watchdog is disabled; DCS loss does not force demotion | Policy state persists in agent memory; socket loss does not add demotion | `pause.rst`, `frozen_patroni.feature`, API feature |
| Failover | Candidate checks, DCS expiry, promotion, history, callbacks, and rewind retain current order | Same elected member, DCS records, callback sequence, and terminal roles | basic replication feature |
| Switchover | Scheduled and immediate requests use current checks, status codes, demotion, and promotion flow | Same responses, target, ordering, and terminal roles | API feature |
| Shutdown | Current active and paused shutdown differ; leader release may occur at PostgreSQL shutdown safepoints | Same branch and no earlier leader deletion | `ha.py` shutdown callbacks and unit tests |
| Watchdog | Timeout derives from TTL and safety margin; keepalive aligns with leader renewal; pause disables it | Agent performs identical activation, reload, keepalive, disable, and failure behavior | `watchdog/base.py`, frozen feature |
| REST status | Handlers query PostgreSQL for each request and combine local and DCS state | Equivalent freshness, retry, status code, headers, and body | `api.py`; API feature |
| REST mutation | Validation and response semantics precede HA work | Same validation, response, scheduling, and cancellation | `api.py`; API feature |
| Dynamic config | DCS config is authoritative; local PostgreSQL config is rendered and reloaded under current rules | Controller filters a typed plan; agent applies the same effective values | `config.py`, `postgresql/config.py` |
| Restart/reinit | Role checks, scheduling, cancellation, PGDATA replacement, and result reporting are current API behavior | Same accepted/rejected requests and terminal state | API feature |
| Replica methods | Configured method order and documented arguments select clone behavior | Same order, arguments, exit handling, and `no_leader` behavior | replica bootstrap docs and features |
| Callbacks | Action, role, scope, ordering, and cancellation follow current executor behavior | Agent runs callbacks with the same documented contract | `callback_executor.py`; feature callback checks |
| Rewind | Timeline checks, credential use, divergence removal, and fallback remain local | Controller requests intent only; agent reproduces outcomes | `rewind.py`; basic replication feature |
| Slots and sync | HA policy and local SQL/files jointly maintain state | Controller plans; agent applies; DCS and PostgreSQL states match | control replication tests; Jepsen sync campaign |
| Process model | Postmaster is orphaned to init and later found through the PID file | Agent uses the same helper, orphaning, discovery, and adoption | `postmaster.py`, `postgresql/__init__.py` |
| Agent loss | No current separate process exists | Stop renewal; never delete leader without stop/fence proof; container exits | authority tests and Jepsen gate |
| Controller loss | No current separate process exists | Active mode follows current DCS-loss path; paused mode preserves state | authority tests and Jepsen gate |

## Required terminal assertions

Every differential scenario compares:

1. primary identity and PostgreSQL role;
2. leader, initialize, sync, config, history, and member DCS records;
3. DCS mutation order;
4. PostgreSQL lifecycle and timeline outcome;
5. REST status, headers, and body;
6. callback and replica-method order and arguments;
7. watchdog state;
8. process adoption state.

An unexplained mismatch blocks the milestone. An intentional mismatch requires
a separate ADR and approval.
