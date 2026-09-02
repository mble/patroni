# M06: Process supervision

Status: complete, 2026-09-02

Implementation: `patroni/agent.py`, `patroni/agent_supervisor.py`,
`patroni/control/authority.py`, `tests/test_agent.py`, and
`tests/test_control_authority.py`.

`patroni-agent` constructs PostgreSQL, recovery, replication, commands, and
watchdog services without constructing a DCS client. It rejects top-level DCS
configuration. The PID-1 supervisor forwards signals, reaps adopted children,
and exits with the agent without respawning it. PostgreSQL still launches via
the existing orphaning helper and is rediscovered through `postmaster.pid` and
`psutil`. Active fallback shutdown stops PostgreSQL through `NodeControl`;
paused shutdown preserves it. Authority checks run on a separate thread ready
for the M07 transport binding.

## Goal

Retain Patroni's postmaster orphan and adoption semantics while making the agent
the sole PostgreSQL lifecycle authority.

## Work

1. Add `patroni-agent`.
2. Add an agent-container PID 1 supervisor.
3. Run the agent daemon as the supervisor's managed child.
4. Retain the existing helper-based postmaster launch and orphaning.
5. Let PID 1 adopt and reap PostgreSQL.
6. Retain PID-file and `psutil` postmaster discovery and adoption.
7. Invoke current active or paused Patroni behaviour on graceful shutdown.
8. On unexpected daemon exit, make PID 1 exit without respawning the daemon.
9. Run authority monitoring independently from command workers.
10. Reject top-level DCS configuration in agent mode.
11. Leave monolithic startup unchanged.

## Tests

- PostgreSQL is reparented to the expected PID 1 supervisor.
- Existing-postmaster discovery and adoption.
- Graceful daemon termination and forced container termination.
- Supervisor and container-runtime response to unexpected daemon exit.
- No daemon-only respawn after failure.
- Orphan and zombie detection by PID 1.
- Restart with stopped, replica, and primary PGDATA.
- Manual promotion detection.
- Recursive command-process cancellation.
- Differential startup and adoption traces against monolithic Patroni.

## Reviews

### Correctness

Postmaster adoption remains valid. Daemon or supervisor failure cannot leave
PostgreSQL running outside the supported container supervision domain. No grant
survives container restart. Signal handling remains safe while commands run.

### Security

Run non-root. Limit mounts, permissions, capabilities, environment, and procfs
access. Confirm no DCS configuration or secret is available.

### Performance

Use child-exit events, PID-file discovery, and bounded timers. Do not add
steady-state process-tree scans beyond current Patroni behaviour.

## Exit

The agent owns lifecycle decisions while preserving current postmaster
orphaning, PID-file discovery, and adoption behaviour. Kubernetes restarts the
whole agent container after daemon failure.
