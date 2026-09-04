import datetime
import unittest

from threading import Event, Thread
from unittest.mock import Mock
from uuid import uuid4

from patroni.control import Freshness, LocalPostgres, NodeSnapshot, ObservationContext, \
    ObservationFailure, PostgresRole, PostgresState, QueryMode, SnapshotDetail, TimelineWal
from patroni.control.node import InProcessNodeControl, PostgresObserver
from patroni.control.postgres import LocalPostgresObserver
from patroni.exceptions import PostgresConnectionException
from patroni.postgresql.misc import PostgresqlRole, PostgresqlState
from patroni.psycopg import OperationalError


class FakeClock:

    def __init__(self) -> None:
        self.value = 10.0

    def __call__(self) -> float:
        return self.value


class FakeObserver(PostgresObserver):

    def __init__(self, local: LocalPostgres, row=None) -> None:
        self.local = local
        self.reads = []
        self.row = row
        self.query_modes = []
        self.query_error = None
        self.invalidations = 0
        self.password = 'must-not-cross'
        self.dsn = 'postgres://secret@localhost/postgres'

    def read(self, detail) -> LocalPostgres:
        if self.reads:
            return self.reads.pop(0)

        return self.local

    def query_status(self, mode: QueryMode):
        self.query_modes.append(mode)
        if self.query_error:
            raise self.query_error

        return self.row

    def invalidate(self):
        self.invalidations += 1

    def replica_timeline(self, leader_timeline):
        return leader_timeline or 2

    def replication_state(self, role, receiver_state, restore_command):
        if role == PostgresRole.PRIMARY:
            return None
        if receiver_state == 'streaming':
            return 'streaming'
        if restore_command:
            return 'in archive recovery'

    def is_primary(self):
        return self.local.observed_role == PostgresRole.PRIMARY

    def is_running(self):
        return self.local.state not in (PostgresState.STOPPED, PostgresState.CRASHED)

    def is_starting(self):
        return self.local.state in (PostgresState.STARTING, PostgresState.BOOTSTRAP_STARTING)

    def last_operation(self):
        return 100

    def timeline_wal(self):
        return TimelineWal(1, 100, 1, 0, 0)

    def current_replication_state(self):
        return 'streaming'

    def received_timeline(self):
        return 1

    def control_timeline(self):
        return 1

    def postmaster_start(self):
        return '2026-01-01 00:00:00+00:00'

    def server_version(self):
        return 180006

    def slots(self):
        return {}

    def timeline_history(self, timeline):
        return ((timeline - 1, '0/10', 'reason'),)

    def checkpoint_locations(self):
        return 20, 10


def local(state=PostgresState.RUNNING, role=PostgresRole.PRIMARY,
          pending_restart=()):
    return LocalPostgres(
        state,
        role,
        role,
        'sysid',
        True,
        pending_restart,
    )


def primary_row():
    return (
        datetime.datetime(2026, 1, 1), 1, 100, 0, None, False,
        None, None, None, None, None, [],
    )


def replica_row():
    return (
        datetime.datetime(2026, 1, 1), 0, 0, 80, 90, False,
        datetime.datetime(2026, 1, 2), 95, 'streaming', None, 92,
        [{'application_name': 'replica', 'client_addr': '127.0.0.1',
          'state': 'streaming', 'sync_state': 'async', 'sync_priority': 0,
          'usename': 'replicator'}],
    )


class TestInProcessNodeControl(unittest.TestCase):

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.agent_id = str(uuid4())
        self.observer = FakeObserver(local(), primary_row())
        self.node = InProcessNodeControl(self.agent_id, self.observer, self.clock)

    def snapshot(self, detail=SnapshotDetail.STATUS,
                 freshness=Freshness.FRESH,
                 context=ObservationContext(None)) -> NodeSnapshot:
        return self.node.snapshot(detail, freshness, context)

    def test_primary_snapshot(self) -> None:
        snapshot = self.snapshot()

        self.assertEqual(PostgresRole.PRIMARY, snapshot.observed_role)
        self.assertEqual(PostgresRole.PRIMARY, snapshot.desired_role)
        self.assertEqual(PostgresState.RUNNING, snapshot.postgres_state)
        self.assertEqual(1, snapshot.timeline)
        self.assertEqual(100, snapshot.wal.location)
        self.assertEqual(ObservationFailure.NONE, snapshot.failure)

    def test_replica_snapshot(self) -> None:
        self.observer.local = local(role=PostgresRole.REPLICA)
        self.observer.row = replica_row()

        snapshot = self.snapshot(context=ObservationContext(3))

        self.assertEqual(PostgresRole.REPLICA, snapshot.observed_role)
        self.assertEqual(3, snapshot.timeline)
        self.assertEqual(92, snapshot.wal.received_location)
        self.assertEqual(80, snapshot.wal.replayed_location)
        self.assertEqual(95, snapshot.latest_end_lsn)
        self.assertEqual('streaming', snapshot.replication_state)
        self.assertEqual('replica', snapshot.replication[0].application_name)

    def test_stopped_snapshot_skips_query(self) -> None:
        self.observer.local = local(PostgresState.STOPPED, PostgresRole.DEMOTED)

        snapshot = self.snapshot()

        self.assertEqual(PostgresState.STOPPED, snapshot.postgres_state)
        self.assertEqual(PostgresRole.DEMOTED, snapshot.observed_role)
        self.assertEqual([], self.observer.query_modes)

    def test_starting_snapshot_queries(self) -> None:
        self.observer.local = local(PostgresState.STARTING, PostgresRole.REPLICA)
        self.observer.row = replica_row()

        snapshot = self.snapshot()

        self.assertEqual(PostgresState.STARTING, snapshot.postgres_state)
        self.assertEqual([QueryMode.ONCE], self.observer.query_modes)

    def test_failed_query_is_not_affirmative(self) -> None:
        self.observer.query_error = PostgresConnectionException('failed')

        snapshot = self.snapshot()

        self.assertEqual(PostgresState.UNKNOWN, snapshot.postgres_state)
        self.assertEqual(PostgresRole.UNKNOWN, snapshot.observed_role)
        self.assertEqual(ObservationFailure.QUERY_FAILED, snapshot.failure)

    def test_retry_mode_matches_freshness(self) -> None:
        self.snapshot(freshness=Freshness.FRESH_RETRY)

        self.assertEqual([QueryMode.RETRY], self.observer.query_modes)

    def test_cached_snapshot_does_not_query_twice(self) -> None:
        first = self.snapshot(freshness=Freshness.CACHED)
        second = self.snapshot(freshness=Freshness.CACHED)

        self.assertIs(first, second)
        self.assertEqual(1, len(self.observer.query_modes))

    def test_invalidated_snapshot_is_not_reused(self) -> None:
        first = self.snapshot(freshness=Freshness.CACHED)
        self.node.invalidate()
        second = self.snapshot(freshness=Freshness.CACHED)

        self.assertIsNot(first, second)
        self.assertEqual(2, len(self.observer.query_modes))
        self.assertEqual(1, self.observer.invalidations)

    def test_fresh_snapshot_requeries(self) -> None:
        first = self.snapshot()
        second = self.snapshot()

        self.assertLess(first.sequence, second.sequence)
        self.assertEqual(2, len(self.observer.query_modes))

    def test_basic_snapshot_skips_status_query(self) -> None:
        snapshot = self.snapshot(SnapshotDetail.BASIC)

        self.assertEqual(PostgresRole.PRIMARY, snapshot.observed_role)
        self.assertEqual([], self.observer.query_modes)

    def test_status_query_does_not_block_process_state(self) -> None:
        entered = Event()
        release = Event()
        completed = Event()
        query_status = self.observer.query_status

        def query(mode):
            entered.set()
            release.wait(1)
            return query_status(mode)

        self.observer.query_status = query
        status = Thread(target=self.snapshot)
        status.start()
        self.assertTrue(entered.wait(1))
        process_state = Thread(target=lambda: (self.node.is_running(), completed.set()))
        process_state.start()

        responsive = completed.wait(0.2)
        release.set()
        status.join(1)
        process_state.join(1)

        self.assertTrue(responsive)

    def test_status_query_does_not_block_basic_snapshot(self) -> None:
        entered = Event()
        release = Event()
        completed = Event()
        query_status = self.observer.query_status

        def query(mode):
            entered.set()
            release.wait(1)
            return query_status(mode)

        self.observer.query_status = query
        status = Thread(target=self.snapshot)
        status.start()
        self.assertTrue(entered.wait(1))
        basic = Thread(target=lambda: (
            self.snapshot(SnapshotDetail.BASIC),
            completed.set(),
        ))
        basic.start()

        responsive = completed.wait(0.2)
        release.set()
        status.join(1)
        basic.join(1)

        self.assertTrue(responsive)

    def test_history_and_checkpoint_reads(self) -> None:
        self.assertEqual(((1, '0/10', 'reason'),), self.node.timeline_history(2))
        self.assertEqual((20, 10), self.node.checkpoint_locations())

    def test_concurrent_change_fails_closed(self) -> None:
        self.observer.reads = [
            local(PostgresState.RUNNING, PostgresRole.PRIMARY),
            local(PostgresState.STOPPING, PostgresRole.PRIMARY),
        ]

        snapshot = self.snapshot()

        self.assertEqual(PostgresState.UNKNOWN, snapshot.postgres_state)
        self.assertEqual(PostgresRole.UNKNOWN, snapshot.observed_role)
        self.assertEqual(ObservationFailure.INCONSISTENT, snapshot.failure)

    def test_retry_recollects_inconsistent_snapshot(self) -> None:
        stable = local(PostgresState.RUNNING, PostgresRole.PRIMARY)
        self.observer.reads = [
            stable,
            local(PostgresState.STOPPING, PostgresRole.PRIMARY),
            stable,
            stable,
        ]

        snapshot = self.snapshot(freshness=Freshness.FRESH_RETRY)

        self.assertEqual(PostgresState.RUNNING, snapshot.postgres_state)
        self.assertEqual(ObservationFailure.NONE, snapshot.failure)
        self.assertEqual(2, len(self.observer.query_modes))

    def test_snapshot_excludes_credentials_and_raw_config(self) -> None:
        text = repr(self.snapshot())

        self.assertNotIn(self.observer.password, text)
        self.assertNotIn(self.observer.dsn, text)
        self.assertFalse(hasattr(self.snapshot(), 'config'))


def postgres() -> Mock:
    postgresql = Mock()
    postgresql.role = PostgresqlRole.PRIMARY
    postgresql.state = PostgresqlState.RUNNING
    postgresql.is_running.return_value = True
    postgresql.pending_restart_reason = {}
    postgresql.sysid = 'sysid'
    postgresql.server_version = 180006
    postgresql.major_version = 180000
    postgresql.wal_name = 'wal'
    postgresql.lsn_name = 'lsn'
    postgresql.wal_flush = '_flush'
    postgresql.supports_multiple_sync = True
    postgresql.POSTMASTER_START_TIME = 'postmaster_start_time'
    postgresql.TL_LSN = 'timeline_lsn'
    return postgresql


class TestLocalPostgresObserver(unittest.TestCase):

    def test_stopped_primary_is_not_observed(self) -> None:
        postgresql = postgres()
        postgresql.state = PostgresqlState.STOPPED
        postgresql.is_running.return_value = None

        local_state = LocalPostgresObserver(postgresql).read(SnapshotDetail.BASIC)

        self.assertEqual(PostgresRole.UNKNOWN, local_state.observed_role)
        self.assertEqual(PostgresRole.PRIMARY, local_state.desired_role)

    def test_live_primary_overrides_stopped_state(self) -> None:
        postgresql = postgres()
        postgresql.state = PostgresqlState.STOPPED

        local_state = LocalPostgresObserver(postgresql).read(SnapshotDetail.BASIC)

        self.assertEqual(PostgresRole.PRIMARY, local_state.observed_role)

    def test_invalidate_resets_postgres_cache(self) -> None:
        postgresql = postgres()

        LocalPostgresObserver(postgresql).invalidate()

        postgresql.reset_cluster_info_state.assert_called_once_with(None)

    def test_status_requires_heartbeat_connection(self) -> None:
        postgresql = postgres()
        heartbeat = Mock()
        heartbeat.get.side_effect = OperationalError
        postgresql.connection_pool.get.return_value = heartbeat

        observer = LocalPostgresObserver(postgresql)

        self.assertRaises(PostgresConnectionException, observer.query_status, QueryMode.ONCE)

    def test_status_falls_back_to_heartbeat(self) -> None:
        postgresql = postgres()
        heartbeat = Mock()
        rest = Mock()
        rest.get.side_effect = OperationalError
        heartbeat.query.return_value = [primary_row()]
        postgresql.connection_pool.get.side_effect = [heartbeat, rest]

        row = LocalPostgresObserver(postgresql).query_status(QueryMode.ONCE)

        self.assertEqual(primary_row(), row)
        heartbeat.query.assert_called_once()

    def test_pending_restart_redacts_secrets(self) -> None:
        postgresql = postgres()
        postgresql.pending_restart_reason = {
            'ssl_passphrase_command': {'old_value': 'old-secret', 'new_value': 'new-secret'},
        }

        local_state = LocalPostgresObserver(postgresql).read(SnapshotDetail.STATUS)

        self.assertEqual('<redacted>', local_state.pending_restart[0].old_value)
        self.assertNotIn('old-secret', repr(local_state))


if __name__ == '__main__':
    unittest.main()
