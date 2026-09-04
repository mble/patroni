import os
import socket
import struct
import tempfile
import unittest

from pathlib import Path
from threading import Event, Thread
from unittest.mock import Mock, patch
from uuid import uuid4

from patroni.control import AuthorityGrant, AuthorityKind, BootstrapState, CheckpointMode, \
    CloneMode, CommandKind, CommandState, ConfigApply, DesiredRole, DivergencePolicy, Freshness, \
    ObservationContext, ObservationFailure, PostgresRole, SnapshotDetail, TimelineWal, Timing
from patroni.control.authority import AuthorityMonitor
from patroni.control.commands import CommandResult, CommandSubmission, CommandValue, \
    EventKind, EventRecord, LifecycleCommand, ReloadMode, StopMode, SubmitState
from patroni.control.config import config_plan
from patroni.control.protocol import Capability, ErrorCode, Hello, \
    PROTOCOL_MAJOR, PROTOCOL_MINOR, ProtocolError, Response
from patroni.control.rpc import AgentClient, AgentRpc
from patroni.control.unix import _path, peer_check, UnixServer


def observation_trace(node):
    return (
        node.is_running(),
        node.is_primary(),
        node.last_operation(),
        node.timeline_wal(),
        node.slots(),
        node.checkpoint_locations(),
    )


def allow_peer(stream):
    return None


def stop_command() -> LifecycleCommand:
    return LifecycleCommand(
        str(uuid4()), CommandKind.STOP, DesiredRole.UNCHANGED, 10,
        StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None,
        BootstrapState.IDLE, None, None,
    )


@unittest.skipUnless(os.name == 'posix', 'requires POSIX')
class TestControlUnix(unittest.TestCase):

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        os.chmod(self.directory, 0o700)
        self.path = str(self.directory / 'agent.sock')
        self.node = Mock()
        self.node.is_running.return_value = True
        self.node.is_primary.return_value = False
        self.node.last_operation.return_value = 42
        self.node.timeline_wal.return_value = TimelineWal(3, 42, 3, 40, 41)
        self.node.slots.return_value = {'replica': 41}
        self.node.checkpoint_locations.return_value = (42, 40)
        self.monitor = AuthorityMonitor()
        self.config_sink = Mock(return_value=ConfigApply.APPLIED)
        self.rpc = AgentRpc(
            self.node, str(uuid4()), lambda: 1.0, self.monitor, Mock(), self.config_sink,
        )
        self.server = UnixServer(self.path, self.rpc.handle, allow_peer)

    def tearDown(self) -> None:
        self.server.close()
        self.monitor.close()
        self.temporary.cleanup()

    def test_real_socket_node_call(self) -> None:
        self.server.start()
        client = AgentClient(self.path, peer_check=allow_peer)

        self.assertTrue(client.is_running())
        self.node.is_running.assert_called_once_with()

    def test_connection_is_reused(self) -> None:
        self.server.close()
        server_peer = Mock()
        client_peer = Mock()
        self.server = UnixServer(self.path, self.rpc.handle, server_peer)
        self.server.start()
        client = AgentClient(self.path, peer_check=client_peer)

        self.assertTrue(client.is_running())
        self.assertFalse(client.is_primary())
        client.close()

        self.assertEqual(1, server_peer.call_count)
        self.assertEqual(1, client_peer.call_count)

    def test_status_snapshot_does_not_block_ha(self) -> None:
        entered = Event()
        release = Event()
        completed = Event()

        def snapshot(*args):
            entered.set()
            release.wait(1)
            raise RuntimeError('status unavailable')

        self.node.snapshot.side_effect = snapshot
        self.server.start()
        client = AgentClient(self.path, peer_check=allow_peer)
        status = Thread(target=client.snapshot, args=(
            SnapshotDetail.STATUS,
            Freshness.FRESH_RETRY,
            ObservationContext(None),
        ))
        status.start()
        self.assertTrue(entered.wait(1))
        ha = Thread(target=lambda: (client.is_running(), completed.set()))
        ha.start()

        responsive = completed.wait(0.2)
        release.set()
        status.join(1)
        ha.join(1)
        client.close()

        self.assertTrue(responsive)

    def test_client_rejects_minor_skew(self) -> None:
        hello = Hello(
            str(uuid4()), PROTOCOL_MAJOR, PROTOCOL_MINOR - 1,
            tuple(Capability), None, 0,
        )
        response = Response('', None, hello)

        with patch.object(AgentClient, '_exchange', return_value=response), \
                self.assertRaises(ProtocolError):
            AgentClient(self.path, peer_check=allow_peer)

    def test_direct_and_split_traces_match(self) -> None:
        direct = observation_trace(self.node)
        self.server.start()

        split = observation_trace(AgentClient(self.path, peer_check=allow_peer))

        self.assertEqual(direct, split)

    def test_authority_grant_uses_transport_sequence(self) -> None:
        self.server.start()
        controller_id = str(uuid4())
        client = AgentClient(self.path, controller_id, peer_check=allow_peer)
        timing = Timing(30.0, 10.0, 10.0, 20.0)
        grant = AuthorityGrant(
            AuthorityKind.LEADER, controller_id, client.agent_boot_id, 1, 99,
            1.0, 2.0, timing,
        )

        client.grant(grant)

    def test_dynamic_configuration_and_telemetry_round_trip(self) -> None:
        self.server.start()
        client = AgentClient(self.path, peer_check=allow_peer)
        plan = config_plan(8, {'postgresql': {'use_slots': False}})

        self.assertEqual(ConfigApply.APPLIED, client.configure(plan))
        telemetry = client.telemetry()

        self.assertEqual(8, telemetry.config_revision)
        self.assertEqual(plan.fingerprint, telemetry.config_fingerprint)
        self.config_sink.assert_called_once_with(plan)

    def test_client_rebinds_after_agent_restart(self) -> None:
        self.server.start()
        client = AgentClient(self.path, peer_check=allow_peer)
        old_agent_id = client.agent_boot_id
        self.server.close()
        self.monitor.close()

        self.monitor = AuthorityMonitor()
        rpc = AgentRpc(self.node, str(uuid4()), lambda: 1.0, self.monitor, Mock(), self.config_sink)
        self.server = UnixServer(self.path, rpc.handle, allow_peer)
        self.server.start()

        self.assertTrue(client.is_running())
        self.assertNotEqual(old_agent_id, client.agent_boot_id)

    def test_agent_accepts_restarted_controller(self) -> None:
        self.server.start()
        first = AgentClient(self.path, peer_check=allow_peer)
        second = AgentClient(self.path, peer_check=allow_peer)

        self.assertEqual(first.agent_boot_id, second.agent_boot_id)
        self.assertTrue(second.is_running())

    def test_command_status_and_events_round_trip(self) -> None:
        self.server.start()
        client = AgentClient(self.path, peer_check=allow_peer)
        command = stop_command()
        running = CommandResult(command, CommandState.RUNNING, CommandValue.NONE, None, None, ())
        submission = CommandSubmission(SubmitState.ACCEPTED, running)
        event = EventRecord(command.command_id, 1, EventKind.SAFEPOINT, None, None)
        self.node.snapshot.return_value = Mock(observed_role=PostgresRole.REPLICA)
        self.node.submit.return_value = submission
        self.node.command_status.return_value = running
        self.node.command_events.return_value = (event,)

        self.assertEqual(submission, client.submit(command))
        self.assertEqual(running, client.command_status(command.command_id))
        self.assertEqual((event,), client.command_events(command.command_id, 0))
        client.command_ack(command.command_id, event.sequence)

        self.node.command_ack.assert_called_once_with(command.command_id, event.sequence)

    def test_active_socket_is_not_replaced(self) -> None:
        self.server.start()
        second = UnixServer(self.path, self.rpc.handle, allow_peer)

        with self.assertRaises(ValueError):
            second.start()

    def test_stale_socket_is_replaced(self) -> None:
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(self.path)
        stale.close()

        self.server.start()

        self.assertTrue(stat_is_socket(self.path))

    def test_accept_failure_removes_socket(self) -> None:
        socket_file = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        socket_file.bind(self.path)
        self.server._listener = Mock(accept=Mock(side_effect=OSError('failed')))
        self.server._inode = os.lstat(self.path).st_ino

        self.server._serve()
        socket_file.close()

        self.assertFalse(os.path.exists(self.path))

    def test_regular_file_is_not_removed(self) -> None:
        Path(self.path).write_text('keep')

        with self.assertRaises(ValueError):
            self.server.start()

        self.assertEqual('keep', Path(self.path).read_text())

    @patch('patroni.control.unix.os.getegid', Mock(return_value=999))
    @patch('patroni.control.unix.os.geteuid', Mock(return_value=999))
    @patch('patroni.control.unix.os.stat')
    def test_root_owned_group_directory_is_safe(self, mock_stat) -> None:
        parent = Mock(st_mode=0o040770, st_uid=0, st_gid=999)
        mock_stat.return_value = parent

        _path('/run/patroni/agent.sock', 0o660)

        parent.st_gid = 1000
        with self.assertRaises(ValueError):
            _path('/run/patroni/agent.sock', 0o660)

        parent.st_gid = 999
        parent.st_mode = 0o040750
        with self.assertRaises(ValueError):
            _path('/run/patroni/agent.sock', 0o660)

    def test_symlink_is_not_removed(self) -> None:
        target = self.directory / 'target'
        target.write_text('keep')
        os.symlink(str(target), self.path)

        with self.assertRaises(ValueError):
            self.server.start()

        self.assertTrue(os.path.islink(self.path))

    def test_close_removes_owned_socket(self) -> None:
        self.server.start()
        self.server.close()

        self.assertFalse(os.path.exists(self.path))

    def test_close_socket_error_is_not_logged(self) -> None:
        stream = Mock()

        def fail_read(size):
            self.server._closed.set()
            raise OSError('socket closed')

        stream.recv.side_effect = fail_read
        self.server._workers.acquire()

        with patch('patroni.control.unix.logger.exception') as log:
            self.server._handle(stream)

        log.assert_not_called()

    def test_status_fails_unknown_when_agent_is_down(self) -> None:
        self.server.start()
        client = AgentClient(self.path, peer_check=allow_peer)
        self.server.close()

        snapshot = client.snapshot(
            SnapshotDetail.STATUS, Freshness.FRESH_RETRY, ObservationContext(None),
        )

        self.assertEqual(PostgresRole.UNKNOWN, snapshot.observed_role)
        self.assertEqual(ObservationFailure.AGENT_UNAVAILABLE, snapshot.failure)

        with self.assertRaises(ProtocolError):
            client.snapshot(SnapshotDetail.BASIC, Freshness.FRESH, ObservationContext(None))


def stat_is_socket(path):
    import stat

    return stat.S_ISSOCK(os.lstat(path).st_mode)


class TestPeerCheck(unittest.TestCase):

    def test_wrong_peer_identity_is_rejected(self) -> None:
        stream = Mock()
        stream.getsockopt.return_value = struct.pack('3i', 1, 3, 2)

        with patch('patroni.control.unix.socket.SO_PEERCRED', 1, create=True), \
                self.assertRaises(ProtocolError) as raised:
            peer_check(1, 2)(stream)

        self.assertEqual(ErrorCode.FORBIDDEN, raised.exception.code)
