# M07: Unix transport

Status: complete, 2026-09-02

Implementation: `patroni/control/protocol.py`, `patroni/control/rpc.py`,
`patroni/control/unix.py`, and the `patroni-agent` socket lifecycle.

The transport uses bounded tagged JSON frames over one-request Unix
connections. The handshake fixes process identities and advertises
capabilities. Both peers verify Linux credentials. Requests are sequenced,
replayable, and deadline-bound. The agent rejects unsafe paths and removes only
its own stale socket. Authority monitoring uses a separate lock and preempts
blocked command waits. Events support bounded long-poll and acknowledgement.

## Goal

Run the established contract safely between separate local processes.

## Work

1. Add bounded codec, server, and client.
2. Validate peer credentials on both sides.
3. Add socket lifecycle and safe stale-socket handling.
4. Add deadlines, reconnects, version negotiation, and capabilities.
5. Reuse service contract tests over real Unix sockets.
6. Add sequenced long-poll command events and acknowledgement.
7. Add parser fuzz inputs.

## Tests

- Partial headers and bodies.
- Oversized, malformed, and invalid UTF-8 frames.
- Unknown versions, fields, enums, and operations.
- Slow clients and worker exhaustion.
- Wrong peer UID/GID.
- Non-socket, symlink, foreign, stale, and active paths.
- Disconnect during every command phase.
- Safepoint event replay, acknowledgement, overrun, and reconnect.
- Adjacent minor-version combinations.

## Reviews

### Correctness

Reconnects and retries preserve idempotency and Patroni event ordering. Partial
I/O cannot produce a command. Protocol errors leave service state unchanged.

### Security

Fuzz the parser. Verify allocation limits, peer identity, error redaction, path
checks, socket permissions, and file-descriptor closure.

### Performance

Local round-trip p99 stays below one percent of the minimum supported HA loop
interval. Worker count and memory remain bounded under hostile clients.

## Exit

Separate controller and agent processes pass the same contract suite.
