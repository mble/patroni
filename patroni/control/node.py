"""Read-only controller boundary for coherent node observations."""
import datetime
import json

from abc import ABC, abstractmethod
from enum import Enum
from threading import RLock
from typing import Callable, cast, Dict, List, Mapping, Optional, Sequence, Tuple, Type, TYPE_CHECKING
from uuid import UUID

from patroni.exceptions import PostgresConnectionException
from patroni.psycopg import Error
from patroni.utils import RetryFailedError

from .commands import AgentCommands, CancelMode, CommandResult, \
    CommandSubmission, EventRecord, LifecycleCommand, RecoveryTarget
from .models import ConfigChange, Freshness, LocalPostgres, NodeSnapshot, ObservationContext, ObservationFailure, \
    PostgresRole, PostgresState, QueryMode, RecoverySnapshot, ReplicationConnection, SlotCapabilities, SnapshotDetail, \
    SyncContext, SyncSnapshot, TimelineWal, WalObservation, WatchdogReload, WatchdogSnapshot, WatchdogTiming

if TYPE_CHECKING:  # pragma: no cover
    from .recovery import PostgresRecovery
    from .replication import PostgresReplication

MAX_CONSISTENCY_ATTEMPTS = 2
MAX_REPLICATION_CONNECTIONS = 4096
QUERY_STATES = frozenset((
    PostgresState.RUNNING,
    PostgresState.RESTARTING,
    PostgresState.STARTING,
))
EMPTY_WAL = WalObservation(None, None, None, None, None)


class PostgresObserver(ABC):
    """Encapsulate local PostgreSQL reads below the collector."""

    @abstractmethod
    def read(self, detail: SnapshotDetail) -> LocalPostgres:
        """Read local process state without SQL."""

    @abstractmethod
    def query_status(self, mode: QueryMode) -> Sequence[object]:
        """Run the current REST status query."""

    @abstractmethod
    def replica_timeline(self, leader_timeline: Optional[int]) -> Optional[int]:
        """Resolve the cached replica timeline."""

    @abstractmethod
    def replication_state(self, role: PostgresRole, receiver_state: Optional[str],
                          restore_command: Optional[str]) -> Optional[str]:
        """Interpret receiver state using current PostgreSQL rules."""

    @abstractmethod
    def is_primary(self) -> bool:
        """Query whether PostgreSQL is out of recovery."""

    @abstractmethod
    def is_running(self) -> bool:
        """Check for a live postmaster."""

    @abstractmethod
    def is_starting(self) -> bool:
        """Check the current startup state."""

    @abstractmethod
    def last_operation(self) -> int:
        """Read the current local WAL position."""

    @abstractmethod
    def timeline_wal(self) -> TimelineWal:
        """Read coherent timeline and WAL positions."""

    @abstractmethod
    def current_replication_state(self) -> Optional[str]:
        """Read the current receiver state."""

    @abstractmethod
    def received_timeline(self) -> Optional[int]:
        """Read the receiver timeline."""

    @abstractmethod
    def control_timeline(self) -> Optional[int]:
        """Read the control-file timeline."""

    @abstractmethod
    def postmaster_start(self) -> Optional[str]:
        """Read postmaster start time."""

    @abstractmethod
    def server_version(self) -> int:
        """Read connected PostgreSQL version."""

    @abstractmethod
    def slots(self) -> Dict[str, int]:
        """Read current replication-slot positions."""


class NodeControl(ABC):
    """Expose bounded local observations to controller code."""

    @abstractmethod
    def snapshot(self, detail: SnapshotDetail, freshness: Freshness,
                 context: ObservationContext) -> NodeSnapshot:
        """Return one coherent local observation."""

    @abstractmethod
    def invalidate(self) -> None:
        """End the current local snapshot cache scope."""

    @abstractmethod
    def close(self) -> None:
        """Stop the local command service."""

    @abstractmethod
    def submit(self, command: LifecycleCommand) -> CommandSubmission:
        """Submit one lifecycle command."""

    @abstractmethod
    def command_status(self, command_id: str) -> Optional[CommandResult]:
        """Return lifecycle command status."""

    @abstractmethod
    def active_command(self) -> Optional[CommandResult]:
        """Return the active lifecycle command."""

    @abstractmethod
    def command_wait(self, command_id: str, timeout: Optional[float]) -> Optional[CommandResult]:
        """Wait for lifecycle command progress."""

    @abstractmethod
    def command_cancel(self, command_id: str) -> Optional[CommandResult]:
        """Cancel a lifecycle command."""

    @abstractmethod
    def command_events(self, command_id: str, after_sequence: int) -> Tuple[EventRecord, ...]:
        """Return lifecycle events after a sequence."""

    @abstractmethod
    def command_ack(self, command_id: str, sequence: int) -> None:
        """Acknowledge lifecycle events."""

    @abstractmethod
    def is_primary(self) -> bool:
        """Return current recovery state."""

    @abstractmethod
    def is_running(self) -> bool:
        """Return postmaster process presence."""

    @abstractmethod
    def is_starting(self) -> bool:
        """Return whether PostgreSQL is starting."""

    @abstractmethod
    def last_operation(self) -> int:
        """Return current WAL position."""

    @abstractmethod
    def timeline_wal(self) -> TimelineWal:
        """Return coherent timeline and WAL positions."""

    @abstractmethod
    def replication_state(self) -> Optional[str]:
        """Return current replication state."""

    @abstractmethod
    def received_timeline(self) -> Optional[int]:
        """Return current receiver timeline."""

    @abstractmethod
    def replica_timeline(self, leader_timeline: Optional[int]) -> Optional[int]:
        """Return cached replica timeline for a leader timeline."""

    @abstractmethod
    def control_timeline(self) -> Optional[int]:
        """Return current control-file timeline."""

    @abstractmethod
    def postmaster_start(self) -> Optional[str]:
        """Return postmaster start time."""

    @abstractmethod
    def server_version(self) -> int:
        """Return connected PostgreSQL version."""

    @abstractmethod
    def slots(self) -> Dict[str, int]:
        """Return current replication-slot positions."""

    @abstractmethod
    def recovery(self) -> RecoverySnapshot:
        """Return agent-owned recovery state."""

    @abstractmethod
    def can_rewind(self) -> bool:
        """Return whether local rewind is available."""

    @abstractmethod
    def rewind_needed(self, target: Optional[RecoveryTarget]) -> bool:
        """Check whether a source requires rewind."""

    @abstractmethod
    def archive_ready(self) -> bool:
        """Return whether shutdown WAL archiving is configured."""

    @abstractmethod
    def can_clone(self, methods: Optional[Sequence[str]]) -> bool:
        """Return whether a replica method can run without a source."""

    @abstractmethod
    def data_empty(self) -> bool:
        """Return whether PGDATA is empty."""

    @abstractmethod
    def controldata(self) -> Dict[str, str]:
        """Return bounded pg_controldata fields."""

    @abstractmethod
    def restored_from_backup(self) -> bool:
        """Return whether backup_label exists."""

    @abstractmethod
    def recovery_conf_exists(self) -> bool:
        """Return whether recovery configuration exists."""

    @abstractmethod
    def check_recovery_conf(self, target: Optional[RecoveryTarget]) -> ConfigChange:
        """Return required recovery configuration action."""

    @abstractmethod
    def cancel(self, mode: CancelMode) -> None:
        """Cancel the active agent subprocess."""

    @abstractmethod
    def reset_cancel(self) -> None:
        """Reset agent subprocess cancellation state."""

    @abstractmethod
    def watchdog(self) -> WatchdogSnapshot:
        """Return local watchdog health."""

    @abstractmethod
    def activate_watchdog(self) -> bool:
        """Activate the local watchdog."""

    @abstractmethod
    def disable_watchdog(self) -> None:
        """Disable the local watchdog."""

    @abstractmethod
    def keepalive_watchdog(self) -> None:
        """Keep the local watchdog alive."""

    @abstractmethod
    def reload_watchdog(self, timing: WatchdogTiming) -> WatchdogReload:
        """Apply ordered watchdog timing."""

    @abstractmethod
    def sync_state(self, context: SyncContext) -> SyncSnapshot:
        """Return current synchronous replication state."""

    @abstractmethod
    def slot_capabilities(self) -> SlotCapabilities:
        """Return local slot-policy capabilities."""


class InProcessNodeControl(NodeControl):
    """Collect snapshots through the same API a future agent client uses."""

    def __init__(self, agent_boot_id: str, observer: PostgresObserver,
                 clock: Callable[[], float], commands: Optional[AgentCommands] = None,
                 recovery: Optional['PostgresRecovery'] = None,
                 replication: Optional['PostgresReplication'] = None) -> None:
        if agent_boot_id != str(UUID(agent_boot_id)):
            raise ValueError('agent_boot_id is not a canonical UUID')

        self._agent_boot_id = agent_boot_id
        self._observer = observer
        self._clock = clock
        self._commands = commands
        self._recovery = recovery
        self._replication = replication
        self._lock = RLock()
        self._sequence = 0
        self._cache: Dict[SnapshotDetail, NodeSnapshot] = {}

    def snapshot(self, detail: SnapshotDetail, freshness: Freshness,
                 context: ObservationContext) -> NodeSnapshot:
        """Return cached or request-time local state."""
        _enum(detail, SnapshotDetail, 'snapshot detail')
        _enum(freshness, Freshness, 'freshness')
        _context(context)

        with self._lock:
            cached = self._cache.get(detail)
            if freshness == Freshness.CACHED and cached:
                return cached

            self._sequence += 1
            query_mode = QueryMode.RETRY if freshness == Freshness.FRESH_RETRY else QueryMode.ONCE
            attempts = MAX_CONSISTENCY_ATTEMPTS if freshness == Freshness.FRESH_RETRY else 1
            snapshot = self._collect(detail, context, query_mode, attempts)
            self._cache[detail] = snapshot
            return snapshot

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()

    def close(self) -> None:
        if self._commands:
            self._commands.close()

    def submit(self, command: LifecycleCommand) -> CommandSubmission:
        return self._command_service().submit(command)

    def command_status(self, command_id: str) -> Optional[CommandResult]:
        return self._command_service().status(command_id)

    def active_command(self) -> Optional[CommandResult]:
        return self._command_service().active()

    def command_wait(self, command_id: str, timeout: Optional[float]) -> Optional[CommandResult]:
        return self._command_service().wait(command_id, timeout)

    def command_cancel(self, command_id: str) -> Optional[CommandResult]:
        return self._command_service().cancel(command_id)

    def command_events(self, command_id: str, after_sequence: int) -> Tuple[EventRecord, ...]:
        return self._command_service().events(command_id, after_sequence)

    def command_ack(self, command_id: str, sequence: int) -> None:
        self._command_service().ack(command_id, sequence)

    def _command_service(self) -> AgentCommands:
        if self._commands is None:
            raise RuntimeError('lifecycle command service is not configured')

        return self._commands

    def is_primary(self) -> bool:
        with self._lock:
            return self._observer.is_primary()

    def is_running(self) -> bool:
        with self._lock:
            return self._observer.is_running()

    def is_starting(self) -> bool:
        with self._lock:
            return self._observer.is_starting()

    def last_operation(self) -> int:
        with self._lock:
            return self._observer.last_operation()

    def timeline_wal(self) -> TimelineWal:
        with self._lock:
            return self._observer.timeline_wal()

    def replication_state(self) -> Optional[str]:
        with self._lock:
            return self._observer.current_replication_state()

    def received_timeline(self) -> Optional[int]:
        with self._lock:
            return self._observer.received_timeline()

    def replica_timeline(self, leader_timeline: Optional[int]) -> Optional[int]:
        with self._lock:
            return self._observer.replica_timeline(leader_timeline)

    def control_timeline(self) -> Optional[int]:
        with self._lock:
            return self._observer.control_timeline()

    def postmaster_start(self) -> Optional[str]:
        with self._lock:
            return self._observer.postmaster_start()

    def server_version(self) -> int:
        with self._lock:
            return self._observer.server_version()

    def slots(self) -> Dict[str, int]:
        with self._lock:
            return self._observer.slots()

    def recovery(self) -> RecoverySnapshot:
        return self._recovery_service().snapshot()

    def can_rewind(self) -> bool:
        return self._recovery_service().can_rewind()

    def rewind_needed(self, target: Optional[RecoveryTarget]) -> bool:
        return self._recovery_service().needed(target)

    def archive_ready(self) -> bool:
        return self._recovery_service().archive_enabled()

    def can_clone(self, methods: Optional[Sequence[str]]) -> bool:
        return self._recovery_service().can_clone(methods)

    def data_empty(self) -> bool:
        return self._recovery_service().data_empty()

    def controldata(self) -> Dict[str, str]:
        return self._recovery_service().controldata()

    def restored_from_backup(self) -> bool:
        return self._recovery_service().restored()

    def recovery_conf_exists(self) -> bool:
        return self._recovery_service().recovery_conf_exists()

    def check_recovery_conf(self, target: Optional[RecoveryTarget]) -> ConfigChange:
        return self._recovery_service().check_recovery_conf(target)

    def cancel(self, mode: CancelMode) -> None:
        self._recovery_service().cancel(mode)

    def reset_cancel(self) -> None:
        self._recovery_service().reset_cancel()

    def watchdog(self) -> WatchdogSnapshot:
        return self._replication_service().watchdog()

    def activate_watchdog(self) -> bool:
        return self._replication_service().activate_watchdog()

    def disable_watchdog(self) -> None:
        self._replication_service().disable_watchdog()

    def keepalive_watchdog(self) -> None:
        self._replication_service().keepalive_watchdog()

    def reload_watchdog(self, timing: WatchdogTiming) -> WatchdogReload:
        return self._replication_service().reload_watchdog(timing)

    def sync_state(self, context: SyncContext) -> SyncSnapshot:
        return self._replication_service().sync_state(context)

    def slot_capabilities(self) -> SlotCapabilities:
        return self._replication_service().slot_capabilities()

    def _recovery_service(self) -> 'PostgresRecovery':
        if self._recovery is None:
            raise RuntimeError('recovery service is not configured')

        return self._recovery

    def _replication_service(self) -> 'PostgresReplication':
        if self._replication is None:
            raise RuntimeError('replication service is not configured')

        return self._replication

    def _collect(self, detail: SnapshotDetail, context: ObservationContext,
                 query_mode: QueryMode, attempts: int) -> NodeSnapshot:
        before = self._observer.read(detail)
        for _ in range(attempts):
            try:
                row = self._observer.query_status(query_mode) \
                    if detail == SnapshotDetail.STATUS and before.state in QUERY_STATES else None
            except (Error, PostgresConnectionException, RetryFailedError):
                return self._failed(detail, before, ObservationFailure.QUERY_FAILED)

            after = self._observer.read(detail)
            if before == after:
                return self._build(detail, context, before, row)

            before = self._observer.read(detail)

        return self._failed(detail, before, ObservationFailure.INCONSISTENT)

    def _build(self, detail: SnapshotDetail, context: ObservationContext,
               local: LocalPostgres, row: Optional[Sequence[object]]) -> NodeSnapshot:
        if row is None:
            return self._snapshot(detail, local, local.observed_role, local.state)
        if len(row) != 12:
            return self._failed(detail, local, ObservationFailure.QUERY_FAILED)

        role = PostgresRole.PRIMARY if bool(row[1]) else PostgresRole.REPLICA
        is_primary = role == PostgresRole.PRIMARY
        timeline = _integer(row[1]) if is_primary else self._observer.replica_timeline(context.leader_timeline)
        wal = WalObservation(
            _integer(row[2]) if is_primary else None,
            None if is_primary else _integer(row[10]) or _integer(row[4]) or _integer(row[3]),
            None if is_primary else _integer(row[3]),
            None if is_primary else _datetime(row[6]),
            None if is_primary else bool(row[5]),
        )

        replication = _replication(row[11])
        if replication is None:
            return self._failed(detail, local, ObservationFailure.LIMIT_EXCEEDED)

        receiver_state = row[8] if isinstance(row[8], str) else None
        restore_command = row[9] if isinstance(row[9], str) else None
        replication_state = self._observer.replication_state(role, receiver_state, restore_command)

        return self._snapshot(
            detail,
            local,
            role,
            local.state,
            timeline,
            wal,
            _integer(row[7]),
            replication_state,
            replication,
            _datetime(row[0]),
            server_version=self._observer.server_version(),
        )

    def _failed(self, detail: SnapshotDetail, local: LocalPostgres,
                failure: ObservationFailure) -> NodeSnapshot:
        state = PostgresState.UNKNOWN if local.state in QUERY_STATES else local.state
        role = PostgresRole.UNKNOWN if local.state in QUERY_STATES else local.observed_role
        return self._snapshot(detail, local, role, state, failure=failure)

    def _snapshot(self, detail: SnapshotDetail, local: LocalPostgres,
                  role: PostgresRole, state: PostgresState,
                  timeline: Optional[int] = None, wal: WalObservation = EMPTY_WAL,
                  latest_end_lsn: Optional[int] = None,
                  replication_state: Optional[str] = None,
                  replication: Tuple[ReplicationConnection, ...] = (),
                  postmaster_start_time: Optional[datetime.datetime] = None,
                  server_version: int = 0,
                  failure: ObservationFailure = ObservationFailure.NONE) -> NodeSnapshot:
        return NodeSnapshot(
            self._agent_boot_id,
            self._sequence,
            self._clock(),
            detail,
            role,
            local.desired_role,
            state,
            local.supports_multiple_sync,
            local.system_identifier,
            server_version if failure == ObservationFailure.NONE else 0,
            timeline,
            wal,
            latest_end_lsn,
            replication_state,
            replication,
            postmaster_start_time,
            local.pending_restart,
            failure,
        )


def _integer(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)

    if isinstance(value, (float, int, str)):
        try:
            return int(value)
        except ValueError:
            return None

    return None


def _datetime(value: object) -> Optional[datetime.datetime]:
    return value if isinstance(value, datetime.datetime) else None


def _replication(value: object) -> Optional[Tuple[ReplicationConnection, ...]]:
    if not value:
        return ()
    if isinstance(value, str):
        try:
            value = cast(object, json.loads(value))
        except (TypeError, ValueError):
            return ()
    if not isinstance(value, list):
        return ()
    items = cast(List[object], value)
    if len(items) > MAX_REPLICATION_CONNECTIONS:
        return None

    connections: List[ReplicationConnection] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        data = cast(Mapping[str, object], item)
        connections.append(ReplicationConnection(
            str(data.get('application_name') or ''),
            str(data['client_addr']) if data.get('client_addr') is not None else None,
            str(data.get('state') or ''),
            str(data.get('sync_state') or ''),
            _integer(data.get('sync_priority')) or 0,
            str(data.get('usename') or ''),
        ))

    return tuple(connections)


def _enum(value: object, enum_type: Type[Enum], name: str) -> None:
    if not isinstance(value, enum_type):
        raise ValueError('invalid {0}'.format(name))


def _context(value: object) -> None:
    if not isinstance(value, ObservationContext):
        raise ValueError('invalid observation context')
