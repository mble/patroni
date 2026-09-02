"""Local PostgreSQL observation driver."""
from collections.abc import Mapping as MappingABC
from typing import Any, cast, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from patroni.exceptions import PostgresConnectionException
from patroni.psycopg import Error
from patroni.utils import Retry

from .models import LocalPostgres, PendingRestart, PostgresRole, PostgresState, QueryMode, SnapshotDetail, TimelineWal
from .node import PostgresObserver

if TYPE_CHECKING:  # pragma: no cover
    from patroni.postgresql import Postgresql


MAX_PENDING_RESTART = 4096
REDACTED = '<redacted>'
SECRET_PARAMETER_PARTS = ('password', 'passphrase', 'secret', 'token')


class LocalPostgresObserver(PostgresObserver):
    """Keep raw PostgreSQL mechanics below the node boundary."""

    def __init__(self, postgresql: 'Postgresql') -> None:
        self._postgresql = postgresql

    def read(self, detail: SnapshotDetail) -> LocalPostgres:
        """Read cached process state and bounded public metadata."""
        role = PostgresRole(self._postgresql.role.value)
        pending_restart = _pending_restart(self._postgresql.pending_restart_reason)

        return LocalPostgres(
            PostgresState(self._postgresql.state.value),
            role,
            role,
            self._postgresql.sysid,
            self._postgresql.supports_multiple_sync,
            pending_restart,
        )

    def query_status(self, mode: QueryMode) -> Sequence[object]:
        """Run the existing request-time REST status query."""
        major_version = self._postgresql.major_version
        replication_state = (
            "pg_catalog.pg_{0}_{1}_diff(wr.latest_end_lsn, '0/0')::bigint, wr.status"
            if major_version >= 90600 else "NULL, NULL"
        ) + ", " + (
            "pg_catalog.current_setting('restore_command')" if major_version >= 120000 else "NULL"
        ) + ", " + (
            "pg_catalog.pg_wal_lsn_diff(wr.written_lsn, '0/0')::bigint"
            if major_version >= 130000 else "NULL"
        )
        statement = (
            "SELECT " + self._postgresql.POSTMASTER_START_TIME + ", " + self._postgresql.TL_LSN + ","
            " pg_catalog.pg_last_xact_replay_timestamp(), " + replication_state + ","
            " (SELECT pg_catalog.array_to_json(pg_catalog.array_agg(pg_catalog.row_to_json(ri))) "
            "FROM (SELECT (SELECT rolname FROM pg_catalog.pg_authid WHERE oid = usesysid) AS usename,"
            " application_name, client_addr, w.state, sync_state, sync_priority"
            " FROM pg_catalog.pg_stat_get_wal_senders() w, pg_catalog.pg_stat_get_activity(pid)) AS ri)"
        ) + (
            " FROM pg_catalog.pg_stat_get_wal_receiver() AS wr" if major_version >= 90600 else ""
        )
        statement = statement.format(
            self._postgresql.wal_name,
            self._postgresql.lsn_name,
            self._postgresql.wal_flush,
        )

        query = self._query
        rows = Retry(delay=1, retry_exceptions=PostgresConnectionException)(query, statement) \
            if mode == QueryMode.RETRY else query(statement)
        if not rows:
            raise PostgresConnectionException('status query returned no rows')

        return rows[0]

    def replica_timeline(self, leader_timeline: Optional[int]) -> Optional[int]:
        """Use Patroni's current replica timeline cache."""
        return self._postgresql.replica_cached_timeline(leader_timeline)

    def replication_state(self, role: PostgresRole, receiver_state: Optional[str],
                          restore_command: Optional[str]) -> Optional[str]:
        """Use Patroni's current receiver-state interpretation."""
        return self._postgresql.replication_state_from_parameters(
            role == PostgresRole.PRIMARY,
            receiver_state,
            restore_command,
        )

    def is_primary(self) -> bool:
        return self._postgresql.is_primary()

    def is_running(self) -> bool:
        return bool(self._postgresql.is_running())

    def is_starting(self) -> bool:
        return self._postgresql.is_starting()

    def last_operation(self) -> int:
        return self._postgresql.last_operation()

    def timeline_wal(self) -> TimelineWal:
        return TimelineWal(*self._postgresql.timeline_wal_position())

    def current_replication_state(self) -> Optional[str]:
        return self._postgresql.replication_state()

    def received_timeline(self) -> Optional[int]:
        return self._postgresql.received_timeline()

    def control_timeline(self) -> Optional[int]:
        return self._postgresql.pg_control_timeline()

    def postmaster_start(self) -> Optional[str]:
        return self._postgresql.postmaster_start_time()

    def server_version(self) -> int:
        return self._postgresql.server_version

    def _query(self, statement: str) -> List[Tuple[object, ...]]:
        """Prefer REST connection and retain heartbeat fallback semantics."""
        try:
            heartbeat = cast(Any, self._postgresql.connection_pool.get('heartbeat'))
            heartbeat.get()
        except Error as exc:
            raise PostgresConnectionException('connection problems') from exc

        try:
            connection = cast(Any, self._postgresql.connection_pool.get('restapi'))
            connection.get()
        except Error:
            connection = heartbeat

        return cast(List[Tuple[object, ...]], connection.query(statement))


def _pending_restart(value: Mapping[str, object]) -> Tuple[PendingRestart, ...]:
    if len(value) > MAX_PENDING_RESTART:
        raise ValueError('pending restart limit exceeded')

    pending: List[PendingRestart] = []
    for name, reason in value.items():
        data: Mapping[str, object]
        if isinstance(reason, MappingABC):
            data = cast(Mapping[str, object], reason)
        else:
            data = cast(Mapping[str, object], {})
        if any(part in name.lower() for part in SECRET_PARAMETER_PARTS):
            old_value = new_value = REDACTED
        else:
            old_value = _scalar(data.get('old_value'))
            new_value = _scalar(data.get('new_value'))
        pending.append(PendingRestart(name, old_value, new_value))

    return tuple(pending)


def _scalar(value: object) -> object:
    if value is None or isinstance(value, (bool, float, int, str)):
        return value

    return str(value)
