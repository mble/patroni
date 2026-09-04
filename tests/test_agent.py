import signal
import sys
import unittest

from unittest.mock import Mock, patch

from patroni.agent import _control_config, _reject_dcs, DCS_SECTIONS, PatroniAgent
from patroni.agent_supervisor import AgentSupervisor, main as supervisor_main, SIGNAL_EXIT_OFFSET
from patroni.control import CommandKind, CommandPhase, CommandState, PolicyMode, SubmitState
from patroni.exceptions import PatroniFatalException


class AgentConfig(dict):

    dynamic_configuration = {}


class TestPatroniAgent(unittest.TestCase):

    def test_dcs_configuration_is_rejected(self) -> None:
        for section in DCS_SECTIONS:
            with self.subTest(section=section), self.assertRaises(PatroniFatalException):
                _reject_dcs({section: {'host': 'dcs'}})

    def test_controller_credentials_are_rejected(self) -> None:
        for section in ('controller', 'restapi', 'ctl'):
            with self.subTest(section=section), self.assertRaises(PatroniFatalException):
                _reject_dcs({section: {'authentication': {'password': 'secret'}}})

    def test_control_socket_must_be_absolute(self) -> None:
        with self.assertRaises(PatroniFatalException):
            _control_config({'agent': {'socket': 'agent.sock'}})

    @patch('patroni.thread_pool.configure_global_pool')
    @patch('patroni.agent.AbstractPatroniDaemon.__init__')
    @patch('patroni.agent.UnixServer')
    @patch('patroni.agent.AgentRpc')
    @patch('patroni.agent.InProcessNodeControl')
    @patch('patroni.agent.AgentCommands')
    @patch('patroni.agent.PostgresCommandDriver')
    @patch('patroni.agent.PostgresReplication')
    @patch('patroni.agent.PostgresRecovery')
    @patch('patroni.agent.Postgresql')
    @patch('patroni.agent.Watchdog')
    @patch('patroni.agent.get_mpp')
    @patch('patroni.agent.global_config.update')
    @patch('patroni.agent.AgentConfigManager')
    def test_agent_constructs_no_dcs_client(self, config_manager, update, get_mpp, watchdog, postgresql,
                                            recovery, replication, driver, commands, node, rpc,
                                            server, daemon_init, configure_pool) -> None:
        config = AgentConfig(
            postgresql={'name': 'node-a'},
            agent={'socket': '/run/patroni/agent.sock'},
            thread_pool_size=7,
        )

        agent = PatroniAgent(config, Mock())

        postgresql.assert_called_once_with(config['postgresql'], get_mpp.return_value)
        config_manager.assert_called_once_with(config, postgresql.return_value)
        self.assertEqual(node.return_value, agent.node)
        server.assert_called_once()
        configure_pool.assert_called_once_with(7)

    @patch('patroni.agent.AbstractPatroniDaemon.__init__')
    def test_agent_requires_control_socket(self, daemon_init) -> None:
        with self.assertRaises(PatroniFatalException):
            PatroniAgent(AgentConfig(postgresql={'name': 'node-a'}), Mock())

    @patch('patroni.agent.AbstractPatroniDaemon.run')
    def test_transport_starts_before_daemon_loop(self, daemon_run) -> None:
        agent = object.__new__(PatroniAgent)
        agent._server = Mock()
        agent.authority = Mock()

        agent.run()

        agent._server.start.assert_called_once_with()
        agent.authority.start.assert_called_once_with()

    def test_active_shutdown_stops_postgres(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent.authority = Mock()
        agent._server = Mock()
        agent.node = Mock()
        agent.config = {'retry_timeout': 1}
        agent._policy = PolicyMode.ACTIVE
        agent._stopping = Mock()
        agent.node.active_command.return_value = None
        agent.node.submit.return_value = Mock(state=SubmitState.ACCEPTED)
        agent.node.command_wait.return_value = Mock(state=CommandState.SUCCEEDED)

        agent._shutdown()

        command = agent.node.submit.call_args.args[0]
        self.assertEqual(CommandKind.STOP, command.kind)
        agent.node.disable_watchdog.assert_called_once_with()
        agent.node.close.assert_called_once_with()

    def test_shutdown_stop_bypasses_stale_phase(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent._stopping = Mock(is_set=Mock(return_value=True))
        agent._rpc = Mock()
        agent.node = Mock()
        agent.node.command_status.return_value = Mock(request=Mock(kind=CommandKind.STOP))

        allowed = agent._command_phase('shutdown-command', CommandPhase.MUTATING)

        self.assertTrue(allowed)
        agent._rpc.phase.assert_not_called()

    def test_shutdown_fences_after_cancel_timeout(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent.node = Mock()
        agent.config = {'retry_timeout': 0}
        active = Mock()
        active.request.command_id = 'active-command'
        agent.node.active_command.return_value = active

        agent._stop_postgres()

        agent.node.command_cancel.assert_called_once_with('active-command')
        agent.node.fence.assert_called_once_with(0.0)

    def test_shutdown_fences_after_stop_failure(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent.node = Mock()
        agent.config = {'retry_timeout': 1}
        agent.node.active_command.return_value = None
        agent.node.submit.return_value = Mock(state=SubmitState.ACCEPTED)
        agent.node.command_wait.return_value = Mock(state=CommandState.FAILED)

        agent._stop_postgres()

        agent.node.fence.assert_called_once_with(1.0)

    def test_paused_shutdown_preserves_postgres(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent.authority = Mock()
        agent._server = Mock()
        agent.node = Mock()
        agent._policy = PolicyMode.PAUSED
        agent._stopping = Mock()

        agent._shutdown()

        agent.node.submit.assert_not_called()
        agent._server.close.assert_called_once_with()
        agent.node.disable_watchdog.assert_called_once_with()


class TestAgentSupervisor(unittest.TestCase):

    @patch('patroni.agent_supervisor.AgentSupervisor')
    def test_main_forwards_config(self, supervisor) -> None:
        supervisor.return_value.run.return_value = 0

        with patch.object(sys, 'argv', ['patroni-agent-supervisor', 'agent.yml']), \
                self.assertRaises(SystemExit) as raised:
            supervisor_main()

        self.assertEqual(0, raised.exception.code)
        supervisor.assert_called_once_with([sys.executable, '-m', 'patroni.agent', 'agent.yml'])

    @patch('patroni.agent_supervisor.os.waitpid')
    def test_reaps_adopted_children(self, waitpid) -> None:
        waitpid.side_effect = [(200, 0), (100, 7 << 8)]

        self.assertEqual(7, AgentSupervisor._reap(100))
        self.assertEqual(2, waitpid.call_count)

    @patch('patroni.agent_supervisor.os.waitpid', return_value=(100, signal.SIGTERM))
    def test_reports_signal_exit(self, waitpid) -> None:
        self.assertEqual(SIGNAL_EXIT_OFFSET + signal.SIGTERM, AgentSupervisor._reap(100))

    @patch('patroni.agent_supervisor.signal.signal')
    @patch('patroni.agent_supervisor.signal.getsignal', return_value=signal.SIG_DFL)
    @patch('patroni.agent_supervisor.os.waitpid', return_value=(100, 0))
    @patch('patroni.agent_supervisor.os.getpid', return_value=1)
    @patch('patroni.agent_supervisor.subprocess.Popen')
    def test_daemon_is_not_respawned(self, popen, getpid, waitpid, getsignal, set_signal) -> None:
        popen.return_value.pid = 100
        supervisor = AgentSupervisor(['patroni-agent', 'agent.yml'])

        self.assertEqual(0, supervisor.run())
        popen.assert_called_once_with(['patroni-agent', 'agent.yml'], close_fds=True)

    def test_empty_command_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            AgentSupervisor([])
