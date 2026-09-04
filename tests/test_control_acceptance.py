import datetime
import multiprocessing
import os
import tempfile
import time
import unittest

from pathlib import Path
from threading import Thread
from uuid import uuid4

from patroni.control import AuthorityGrant, AuthorityKind, BootstrapState, CheckpointMode, CloneMode, \
    CommandKind, CommandPhase, CommandState, DesiredRole, DivergencePolicy, Freshness, LocalPostgres, \
    ObservationContext, PostgresRole, PostgresState, QueryMode, SnapshotDetail, TimelineWal, Timing
from patroni.control.authority import AuthorityMonitor
from patroni.control.commands import AgentCommands, CommandDriver, CommandValue, \
    DriverResult, EventKind, LifecycleCommand, ReloadMode, StopMode, SubmitState
from patroni.control.node import InProcessNodeControl, PostgresObserver
from patroni.control.rpc import AgentClient, AgentRpc
from patroni.control.unix import UnixServer

PROCESS_TIMEOUT = 10.0
RESPONSE_TIMEOUT = 2.0
STATUS_CALLS = 32
SERVER_WORKERS = 2
TIMING = Timing(30.0, 10.0, 10.0, 20.0)
PRIMARY_COMMANDS = (
    (CommandKind.START, AuthorityKind.LEADER),
    (CommandKind.PROMOTE, AuthorityKind.LEADER),
    (CommandKind.RESTART, AuthorityKind.LEADER),
    (CommandKind.BOOTSTRAP, AuthorityKind.INITIALIZER),
)


def allow_peer(stream) -> None:
    pass


class ProcessObserver(PostgresObserver):

    def __init__(self, entered=None, release=None) -> None:
        self.entered = entered
        self.release = release
        self.local = LocalPostgres(
            PostgresState.RUNNING,
            PostgresRole.REPLICA,
            PostgresRole.REPLICA,
            'process-sysid',
            True,
            (),
        )

    def read(self, detail) -> LocalPostgres:
        return self.local

    def query_status(self, mode: QueryMode):
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            self.release.wait(PROCESS_TIMEOUT)

        return (
            datetime.datetime(2026, 1, 1), 0, 0, 0, 0, False,
            None, None, 'streaming', None, 0, [],
        )

    def invalidate(self) -> None:
        pass

    def replica_timeline(self, leader_timeline):
        return leader_timeline or 1

    def replication_state(self, role, receiver_state, restore_command):
        return receiver_state

    def is_primary(self) -> bool:
        return False

    def is_running(self) -> bool:
        return True

    def is_starting(self) -> bool:
        return False

    def last_operation(self) -> int:
        return 0

    def timeline_wal(self) -> TimelineWal:
        return TimelineWal(1, 0, 1, 0, 0)

    def current_replication_state(self):
        return 'streaming'

    def received_timeline(self):
        return 1

    def control_timeline(self):
        return 1

    def postmaster_start(self):
        return None

    def server_version(self) -> int:
        return 180000

    def slots(self):
        return {}

    def timeline_history(self, timeline):
        return ()

    def checkpoint_locations(self):
        return 0, 0


class AckDriver(CommandDriver):

    def __init__(self, entered) -> None:
        self.entered = entered

    def run(self, command, events, cancelled) -> DriverResult:
        event = events.publish(EventKind.SAFEPOINT)
        self.entered.set()
        events.wait_ack(event.sequence, PROCESS_TIMEOUT, cancelled)

        return DriverResult(CommandValue.TRUE, None, None, ())

    def cancel(self) -> None:
        pass

    def fence(self, timeout) -> bool:
        return True


class ImmediateDriver(CommandDriver):

    def __init__(self, returned) -> None:
        self.returned = returned

    def run(self, command, events, cancelled) -> DriverResult:
        self.returned.set()
        return DriverResult(CommandValue.TRUE, None, None, ())

    def cancel(self) -> None:
        pass


class PhaseGate:

    def __init__(self, rpc, target, entered, release) -> None:
        self.rpc = rpc
        self.target = target
        self.entered = entered
        self.release = release
        self.blocked = False

    def __call__(self, command_id, phase) -> bool:
        if not self.blocked and self.target == CommandPhase.ACCEPTED \
                and phase == CommandPhase.PREPARING:
            self._block()

        allowed = self.rpc.phase(command_id, phase)
        if not self.blocked and self.target == phase:
            self._block()

        return allowed

    def _block(self) -> None:
        self.blocked = True
        self.entered.set()
        self.release.wait(PROCESS_TIMEOUT)


def serve_observations(path, entered, release, ready, stop) -> None:
    observer = ProcessObserver(entered, release)
    node = InProcessNodeControl(str(uuid4()), observer, time.monotonic)
    monitor = AuthorityMonitor()
    rpc = AgentRpc(node, str(uuid4()), time.monotonic, monitor, lambda mode: None)
    server = UnixServer(path, rpc.handle, allow_peer, max_workers=SERVER_WORKERS)
    try:
        server.start()
        ready.set()
        stop.wait(PROCESS_TIMEOUT)
    finally:
        server.close()
        monitor.close()
        node.close()


def serve_commands(path, mode, target, entered, release, returned, ready, stop) -> None:
    observer = ProcessObserver()
    driver = AckDriver(entered) if mode == 'ack' else ImmediateDriver(returned)
    commands = AgentCommands(driver)
    node = InProcessNodeControl(str(uuid4()), observer, time.monotonic, commands)
    monitor = AuthorityMonitor()
    rpc = AgentRpc(node, str(uuid4()), time.monotonic, monitor, lambda policy: None)
    if mode == 'phase':
        commands.bind_phase(PhaseGate(rpc, target, entered, release))
    else:
        commands.bind_phase(rpc.phase)
    server = UnixServer(path, rpc.handle, allow_peer)
    try:
        server.start()
        monitor.start()
        ready.set()
        stop.wait(PROCESS_TIMEOUT)
    finally:
        server.close()
        monitor.close()
        node.close()


def command(kind=CommandKind.STOP) -> LifecycleCommand:
    role = DesiredRole.PRIMARY if kind in dict(PRIMARY_COMMANDS) else DesiredRole.UNCHANGED
    return LifecycleCommand(
        str(uuid4()), kind, role, RESPONSE_TIMEOUT,
        StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None,
        BootstrapState.IDLE, None, None,
    )


@unittest.skipUnless('fork' in multiprocessing.get_all_start_methods(), 'requires fork')
class ProcessCase(unittest.TestCase):

    def setUp(self) -> None:
        self.context = multiprocessing.get_context('fork')
        self.temporary = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temporary.name) / 'agent.sock')
        os.chmod(self.temporary.name, 0o700)
        self.process = None
        self.client = None
        self.stop = self.context.Event()
        self.release = self.context.Event()

    def tearDown(self) -> None:
        self.release.set()
        if self.client is not None:
            self.client.close()
        self.stop.set()
        if self.process is not None:
            self.process.join(RESPONSE_TIMEOUT)
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(RESPONSE_TIMEOUT)
        self.temporary.cleanup()

    def start(self, target, args) -> None:
        ready = self.context.Event()
        self.process = self.context.Process(target=target, args=args + (ready, self.stop))
        self.process.start()
        self.assertTrue(ready.wait(RESPONSE_TIMEOUT), self.process.exitcode)
        self.client = AgentClient(self.path, peer_check=allow_peer, timeout=RESPONSE_TIMEOUT)

    def grant(self, kind: AuthorityKind) -> None:
        now = time.monotonic()
        self.client.grant(AuthorityGrant(
            kind,
            self.client.controller_boot_id,
            self.client.agent_boot_id,
            1,
            1,
            now,
            now + TIMING.watchdog_timeout,
            TIMING,
        ))


class TestProcessObservations(ProcessCase):

    def setUp(self) -> None:
        super().setUp()
        self.entered = self.context.Event()
        self.start(serve_observations, (self.path, self.entered, self.release))

    def test_blocked_status_does_not_block_basic(self) -> None:
        status = Thread(target=self.client.snapshot, args=(
            SnapshotDetail.STATUS,
            Freshness.FRESH_RETRY,
            ObservationContext(None),
        ))
        status.start()
        self.assertTrue(self.entered.wait(RESPONSE_TIMEOUT))

        snapshot = self.client.snapshot(
            SnapshotDetail.BASIC,
            Freshness.FRESH,
            ObservationContext(None),
        )
        self.release.set()
        status.join(RESPONSE_TIMEOUT)

        self.assertEqual(PostgresRole.REPLICA, snapshot.observed_role)
        self.assertFalse(status.is_alive())

    def test_status_saturation_survives_reconnect(self) -> None:
        self.grant(AuthorityKind.LEADER)
        workers = [Thread(target=self.client.snapshot, args=(
            SnapshotDetail.STATUS,
            Freshness.FRESH_RETRY,
            ObservationContext(None),
        )) for _ in range(STATUS_CALLS)]
        for worker in workers:
            worker.start()
        self.assertTrue(self.entered.wait(RESPONSE_TIMEOUT))

        self.client._close_stream()
        self.grant(AuthorityKind.LEADER)
        self.release.set()
        for worker in workers:
            worker.join(PROCESS_TIMEOUT)
            self.assertFalse(worker.is_alive())

        self.client._close_stream()
        self.client._close_status_stream()
        self.assertTrue(self.client.is_running())
        snapshot = self.client.snapshot(
            SnapshotDetail.STATUS,
            Freshness.FRESH,
            ObservationContext(None),
        )
        self.assertEqual(PostgresRole.REPLICA, snapshot.observed_role)


class TestProcessCommands(ProcessCase):

    def start_commands(self, mode, target=None):
        entered = self.context.Event()
        returned = self.context.Event()
        self.start(serve_commands, (
            self.path, mode, target, entered, self.release, returned,
        ))
        return entered, returned

    def test_controller_rebinds_during_every_phase(self) -> None:
        for kind, authority in PRIMARY_COMMANDS:
            for phase in CommandPhase:
                with self.subTest(kind=kind, phase=phase):
                    self.tearDown()
                    self.setUp()
                    entered, _ = self.start_commands('phase', phase)
                    self.grant(authority)
                    request = command(kind)
                    submission = self.client.submit(request)
                    self.assertEqual(SubmitState.ACCEPTED, submission.state)
                    self.assertTrue(entered.wait(RESPONSE_TIMEOUT))

                    self.client.close()
                    self.client = AgentClient(
                        self.path,
                        peer_check=allow_peer,
                        timeout=RESPONSE_TIMEOUT,
                    )
                    self.release.set()
                    result = self.client.command_wait(request.command_id, PROCESS_TIMEOUT)
                    self.assertIsNotNone(result)
                    self.assertEqual(CommandState.SUCCEEDED, result.state)

                    self.grant(authority)
                    next_request = command(kind)
                    next_result = self.client.submit(next_request)
                    self.assertEqual(SubmitState.ACCEPTED, next_result.state)
                    result = self.client.command_wait(next_request.command_id, PROCESS_TIMEOUT)
                    self.assertIsNotNone(result)
                    self.assertEqual(CommandState.SUCCEEDED, result.state)

    def test_controller_rebinds_after_local_completion(self) -> None:
        _, returned = self.start_commands('phase')
        self.grant(AuthorityKind.LEADER)
        request = command(CommandKind.PROMOTE)
        self.assertEqual(SubmitState.ACCEPTED, self.client.submit(request).state)
        self.assertTrue(returned.wait(RESPONSE_TIMEOUT))

        self.client.close()
        self.client = AgentClient(self.path, peer_check=allow_peer, timeout=RESPONSE_TIMEOUT)
        result = self.client.command_wait(request.command_id, PROCESS_TIMEOUT)

        self.assertIsNotNone(result)
        self.assertEqual(CommandState.SUCCEEDED, result.state)

        next_request = command()
        self.assertEqual(SubmitState.ACCEPTED, self.client.submit(next_request).state)
        result = self.client.command_wait(next_request.command_id, PROCESS_TIMEOUT)
        self.assertIsNotNone(result)
        self.assertEqual(CommandState.SUCCEEDED, result.state)

    def test_cancel_and_fence_wake_ack_wait(self) -> None:
        for action in ('cancel', 'fence'):
            with self.subTest(action=action):
                self.tearDown()
                self.setUp()
                entered, _ = self.start_commands('ack')
                request = command()
                self.assertEqual(SubmitState.ACCEPTED, self.client.submit(request).state)
                self.assertTrue(entered.wait(RESPONSE_TIMEOUT))

                if action == 'cancel':
                    self.client.command_cancel(request.command_id)
                    expected = CommandState.CANCELLED
                else:
                    self.client.fence(RESPONSE_TIMEOUT)
                    expected = CommandState.FENCED
                result = self.client.command_wait(request.command_id, RESPONSE_TIMEOUT)

                self.assertIsNotNone(result)
                self.assertEqual(expected, result.state)


if __name__ == '__main__':
    unittest.main()
