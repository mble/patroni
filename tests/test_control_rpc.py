import unittest

from threading import Event, Thread
from unittest.mock import Mock
from uuid import uuid4

from patroni.control import AuthorityGrant, AuthorityKind, BootstrapState, CheckpointMode, CloneMode, \
    CommandKind, CommandState, DesiredRole, DivergencePolicy, PolicyMode, PostgresRole, SafetyAction, Timing
from patroni.control.authority import AuthorityMonitor
from patroni.control.commands import CommandResult, CommandSubmission, \
    CommandValue, LifecycleCommand, ReloadMode, StopMode, SubmitState
from patroni.control.protocol import ErrorCode, Hello, NodeCall, Operation, Request
from patroni.control.rpc import AgentRpc


class FakeClock:

    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def command() -> LifecycleCommand:
    return LifecycleCommand(
        str(uuid4()), CommandKind.PROMOTE, DesiredRole.PRIMARY, 10,
        StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None,
        BootstrapState.IDLE, None, None,
    )


class TestAgentRpc(unittest.TestCase):

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.controller_id = str(uuid4())
        self.agent_id = str(uuid4())
        self.node = Mock()
        self.node.is_running.return_value = True
        self.node.snapshot.return_value = Mock(observed_role=PostgresRole.REPLICA)
        self.monitor = Mock()
        self.rpc = AgentRpc(self.node, self.agent_id, self.clock, self.monitor, Mock())
        response = self.rpc.handle(self.request(Operation.HELLO, None, 1, agent_id=''))
        self.assertIsNone(response.error)
        self.assertIsInstance(response.body, Hello)

    def request(self, operation, body, sequence, request_id=None, agent_id=None) -> Request:
        return Request(
            request_id or str(uuid4()), operation, self.controller_id,
            self.agent_id if agent_id is None else agent_id, sequence, body,
        )

    def test_replay_is_idempotent(self) -> None:
        request = self.request(Operation.CALL, (NodeCall.IS_RUNNING, ()), 2)

        first = self.rpc.handle(request)
        second = self.rpc.handle(request)

        self.assertEqual(first, second)
        self.node.is_running.assert_called_once_with()

    def test_request_id_conflict_is_rejected(self) -> None:
        request_id = str(uuid4())
        self.rpc.handle(self.request(Operation.CALL, (NodeCall.IS_RUNNING, ()), 2, request_id))

        response = self.rpc.handle(self.request(Operation.CALL, (NodeCall.IS_PRIMARY, ()), 3, request_id))

        self.assertEqual(ErrorCode.CONFLICT, response.error)
        self.node.is_primary.assert_not_called()

    def test_stale_sequence_is_rejected(self) -> None:
        self.rpc.handle(self.request(Operation.CALL, (NodeCall.IS_RUNNING, ()), 3))

        response = self.rpc.handle(self.request(Operation.CALL, (NodeCall.IS_RUNNING, ()), 2))

        self.assertEqual(ErrorCode.STALE, response.error)

    def test_zero_argument_call_rejects_arguments(self) -> None:
        response = self.rpc.handle(self.request(Operation.CALL, (NodeCall.IS_RUNNING, ('extra',)), 2))

        self.assertEqual(ErrorCode.BAD_REQUEST, response.error)
        self.node.is_running.assert_not_called()

    def test_authority_expiry_preempts_busy_command(self) -> None:
        timing = Timing(30.0, 10.0, 10.0, 20.0)
        grant = AuthorityGrant(
            AuthorityKind.LEADER, self.controller_id, self.agent_id, 1, 2,
            self.clock(), self.clock() + 1, timing,
        )
        self.assertIsNone(self.rpc.handle(self.request(Operation.GRANT, grant, 2)).error)
        lifecycle = command()
        running = CommandResult(lifecycle, CommandState.RUNNING, CommandValue.NONE, None, None, ())
        self.node.submit.return_value = CommandSubmission(SubmitState.ACCEPTED, running)
        self.assertIsNone(self.rpc.handle(self.request(Operation.SUBMIT, (lifecycle, 1), 3)).error)

        self.clock.value += 1
        guard, fence = self.monitor.bind.call_args.args
        self.assertEqual(SafetyAction.FENCE, guard())
        fence()

        self.node.fence.assert_called_once_with(10.0)

    def test_grant_sequence_matches_envelope(self) -> None:
        timing = Timing(30.0, 10.0, 10.0, 20.0)
        grant = AuthorityGrant(
            AuthorityKind.LEADER, self.controller_id, self.agent_id, 1, 1,
            self.clock(), self.clock() + 1, timing,
        )

        response = self.rpc.handle(self.request(Operation.GRANT, grant, 2))

        self.assertEqual(ErrorCode.BAD_REQUEST, response.error)

    def test_paused_policy_suppresses_expiry_fence(self) -> None:
        response = self.rpc.handle(self.request(Operation.POLICY, PolicyMode.PAUSED, 2))
        self.node.snapshot.return_value = Mock(observed_role=PostgresRole.PRIMARY)
        lifecycle = command()
        self.rpc.handle(self.request(Operation.SUBMIT, (lifecycle, 0), 3))

        guard, _ = self.monitor.bind.call_args.args

        self.assertIsNone(response.error)
        self.assertEqual(SafetyAction.NONE, guard())

    def test_wait_does_not_delay_expiry_fence(self) -> None:
        entered = Event()
        release = Event()
        fenced = Event()
        monitor = AuthorityMonitor(0.01)
        node = Mock()
        node.snapshot.return_value = Mock(observed_role=PostgresRole.REPLICA)
        node.fence.side_effect = lambda timeout: fenced.set() or True
        rpc = AgentRpc(node, self.agent_id, self.clock, monitor, Mock())
        self.assertIsNone(rpc.handle(self.request(Operation.HELLO, None, 1, agent_id='')).error)
        timing = Timing(30.0, 10.0, 10.0, 20.0)
        grant = AuthorityGrant(
            AuthorityKind.LEADER, self.controller_id, self.agent_id, 1, 2,
            self.clock(), self.clock() + 1, timing,
        )
        self.assertIsNone(rpc.handle(self.request(Operation.GRANT, grant, 2)).error)
        lifecycle = command()
        running = CommandResult(lifecycle, CommandState.RUNNING, CommandValue.NONE, None, None, ())
        node.submit.return_value = CommandSubmission(SubmitState.ACCEPTED, running)
        self.assertIsNone(rpc.handle(self.request(Operation.SUBMIT, (lifecycle, 1), 3)).error)

        def wait_command(command_id, timeout):
            entered.set()
            release.wait(1)
            return running

        node.command_wait.side_effect = wait_command
        worker = Thread(target=rpc.handle, args=(self.request(
            Operation.COMMAND_WAIT, (lifecycle.command_id, 1.0), 4,
        ),))
        monitor.start()
        worker.start()
        self.assertTrue(entered.wait(1))
        self.clock.value += 1
        monitor.wake()

        self.assertTrue(fenced.wait(0.2))

        release.set()
        worker.join(1)
        monitor.close()


if __name__ == '__main__':
    unittest.main()
