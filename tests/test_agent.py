import signal
import unittest

from unittest.mock import Mock, patch

from patroni.agent import _reject_dcs, DCS_SECTIONS, PatroniAgent
from patroni.agent_supervisor import AgentSupervisor, SIGNAL_EXIT_OFFSET
from patroni.control import CommandKind, CommandState, PolicyMode, SubmitState
from patroni.exceptions import PatroniFatalException


class AgentConfig(dict):

    dynamic_configuration = {}


class TestPatroniAgent(unittest.TestCase):

    def test_dcs_configuration_is_rejected(self) -> None:
        for section in DCS_SECTIONS:
            with self.subTest(section=section), self.assertRaises(PatroniFatalException):
                _reject_dcs({section: {'host': 'dcs'}})

    @patch('patroni.agent.AbstractPatroniDaemon.__init__')
    @patch('patroni.agent.InProcessNodeControl')
    @patch('patroni.agent.AgentCommands')
    @patch('patroni.agent.PostgresCommandDriver')
    @patch('patroni.agent.PostgresReplication')
    @patch('patroni.agent.PostgresRecovery')
    @patch('patroni.agent.Postgresql')
    @patch('patroni.agent.Watchdog')
    @patch('patroni.agent.get_mpp')
    @patch('patroni.agent.global_config.update')
    def test_agent_constructs_no_dcs_client(self, update, get_mpp, watchdog, postgresql,
                                            recovery, replication, driver, commands, node, daemon_init) -> None:
        config = AgentConfig(postgresql={'name': 'node-a'})

        agent = PatroniAgent(config, Mock())

        postgresql.assert_called_once_with(config['postgresql'], get_mpp.return_value)
        self.assertEqual(node.return_value, agent.node)

    def test_active_shutdown_stops_postgres(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent.authority = Mock()
        agent.node = Mock()
        agent.config = {'retry_timeout': 1}
        agent._policy = PolicyMode.ACTIVE
        agent.node.active_command.return_value = None
        agent.node.submit.return_value = Mock(state=SubmitState.ACCEPTED)
        agent.node.command_wait.return_value = Mock(state=CommandState.SUCCEEDED)

        agent._shutdown()

        command = agent.node.submit.call_args.args[0]
        self.assertEqual(CommandKind.STOP, command.kind)
        agent.node.disable_watchdog.assert_called_once_with()
        agent.node.close.assert_called_once_with()

    def test_paused_shutdown_preserves_postgres(self) -> None:
        agent = object.__new__(PatroniAgent)
        agent.authority = Mock()
        agent.node = Mock()
        agent._policy = PolicyMode.PAUSED

        agent._shutdown()

        agent.node.submit.assert_not_called()
        agent.node.disable_watchdog.assert_called_once_with()


class TestAgentSupervisor(unittest.TestCase):

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
