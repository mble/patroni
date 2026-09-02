import os
import socket
import struct
import tempfile
import unittest

from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from patroni.control import AuthorityGrant, AuthorityKind, BootstrapState, CheckpointMode, \
    CloneMode, CommandKind, CommandState, DesiredRole, DivergencePolicy, PostgresRole, Timing
from patroni.control.authority import AuthorityMonitor
from patroni.control.commands import CommandResult, CommandSubmission, CommandValue, \
    EventKind, EventRecord, LifecycleCommand, ReloadMode, StopMode, SubmitState
from patroni.control.protocol import ErrorCode, ProtocolError
from patroni.control.rpc import AgentClient, AgentRpc
from patroni.control.unix import peer_check, UnixServer


def allow_peer(stream):
    return None


def stop_command() -> LifecycleCommand:
    return LifecycleCommand(
        str(uuid4()), CommandKind.STOP, DesiredRole.UNCHANGED, 10,
        StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None,
        BootstrapState.IDLE, None, None,
    )


class TestControlUnix(unittest.TestCase):

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        os.chmod(self.directory, 0o700)
        self.path = str(self.directory / 'agent.sock')
        self.node = Mock()
        self.node.is_running.return_value = True
        self.monitor = AuthorityMonitor()
        self.rpc = AgentRpc(
            self.node, str(uuid4()), lambda: 1.0, self.monitor, Mock(),
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

    def test_regular_file_is_not_removed(self) -> None:
        Path(self.path).write_text('keep')

        with self.assertRaises(ValueError):
            self.server.start()

        self.assertEqual('keep', Path(self.path).read_text())

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
