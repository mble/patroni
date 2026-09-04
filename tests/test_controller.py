import os
import subprocess
import sys
import unittest

from unittest.mock import Mock, patch

from patroni.config import Config
from patroni.control import AuthorityKind, PostgresRole, PostgresState
from patroni.controller import _config_revision, _controller_config, ControllerPostgresql, PatroniController
from patroni.dcs import Cluster
from patroni.exceptions import PatroniFatalException
from patroni.postgresql.misc import PostgresqlRole, PostgresqlState
from patroni.postgresql.mpp import Null


class TestControllerConfig(unittest.TestCase):

    def test_accepts_decimal_dcs_revision(self) -> None:
        cluster = Mock(config=Mock(modify_version='123'))

        self.assertEqual(123, _config_revision(cluster))

    def test_real_config_needs_no_agent_private_values(self) -> None:
        document = """
name: node-a
scope: cluster-a
controller:
  socket: {0}
etcd3:
  host: etcd:2379
postgresql:
  connect_address: postgres:5432
restapi:
  listen: 0.0.0.0:8008
  connect_address: node-a:8008
""".format(os.path.abspath('agent.sock'))
        with patch.dict('os.environ', {Config.PATRONI_CONFIG_VARIABLE: document}):
            config = Config('')

        result = _controller_config(config.local_configuration)

        self.assertEqual(os.path.abspath('agent.sock'), result.socket)

    def test_controller_import_needs_no_psycopg(self) -> None:
        script = """
import builtins
original = builtins.__import__
def blocked(name, globals=None, locals=None, fromlist=(), level=0):
    if level == 0 and name.split('.')[0] in ('psycopg', 'psycopg2'):
        raise ImportError(name)
    return original(name, globals, locals, fromlist, level)
builtins.__import__ = blocked
import patroni.controller
"""
        result = subprocess.run(
            [sys.executable, '-c', script], capture_output=True, text=True,
        )

        self.assertEqual('', result.stderr)
        self.assertEqual(0, result.returncode)

    def test_requires_agent_socket_and_etcd3(self) -> None:
        with self.assertRaises(PatroniFatalException):
            _controller_config({})
        with self.assertRaises(PatroniFatalException):
            _controller_config({'controller': {'socket': '/run/patroni/agent.sock'}})
        with self.assertRaises(PatroniFatalException):
            _controller_config({'controller': {'socket': 'agent.sock'}, 'etcd3': {'host': 'etcd:2379'}})

    def test_rejects_postgres_secrets_and_paths(self) -> None:
        base = {
            'controller': {'socket': os.path.abspath('agent.sock')},
            'etcd3': {'hosts': ['etcd:2379']},
        }
        for key in ('authentication', 'data_dir', 'bin_dir', 'pgpass'):
            config = dict(base, postgresql={key: 'secret'})
            with self.subTest(key=key), self.assertRaises(PatroniFatalException):
                _controller_config(config)

    def test_rejects_agent_only_configuration(self) -> None:
        base = {
            'controller': {'socket': os.path.abspath('agent.sock')},
            'etcd3': {'hosts': ['etcd:2379']},
        }
        documents = (
            dict(base, postgresql={'parameters': {'max_connections': 200}}),
            dict(base, bootstrap={'dcs': {}, 'initdb': ['data-checksums']}),
            dict(base, watchdog={'mode': 'automatic', 'device': '/dev/watchdog'}),
            dict(base, citus={'database': 'citus', 'group': 0}),
        )
        for config in documents:
            with self.subTest(config=config), self.assertRaises(PatroniFatalException):
                _controller_config(config)


class TestControllerPostgresql(unittest.TestCase):

    def setUp(self) -> None:
        self.node = Mock()
        self.snapshot = Mock(
            desired_role=PostgresRole.REPLICA,
            postgres_state=PostgresState.RUNNING,
        )
        self.node.snapshot.return_value = self.snapshot
        self.postgresql = ControllerPostgresql({
            'name': 'node-a',
            'scope': 'cluster-a',
            'connect_address': 'postgres:5432',
            'database': 'postgres',
        }, Null(), self.node)

    def test_exposes_public_member_state(self) -> None:
        self.assertEqual('node-a', self.postgresql.name)
        self.assertEqual('postgres://postgres:5432/postgres', self.postgresql.connection_string)
        self.assertEqual(PostgresqlRole.REPLICA, self.postgresql.role)
        self.assertEqual(PostgresqlState.RUNNING, self.postgresql.state)
        self.assertTrue(self.postgresql.is_healthy())

    def test_local_state_hints_do_not_mutate_agent(self) -> None:
        self.postgresql.set_role(PostgresqlRole.DEMOTED)
        self.postgresql.set_state(PostgresqlState.STOPPED)

        self.assertEqual(PostgresqlRole.DEMOTED, self.postgresql.role)
        self.assertEqual(PostgresqlState.STOPPED, self.postgresql.state)
        self.node.submit.assert_not_called()

    def test_health_checks_postmaster(self) -> None:
        self.node.is_running.return_value = False

        self.assertFalse(self.postgresql.is_healthy())


class TestPatroniController(unittest.TestCase):

    @patch('patroni.controller.PatroniController.setup_signal_handlers')
    @patch('patroni.controller.PatroniController.apply_dynamic_configuration')
    @patch('patroni.controller.PatroniController.ensure_unique_name')
    @patch('patroni.controller.PatroniController.ensure_dcs_access', return_value=Cluster.empty())
    @patch('patroni.ha.Ha')
    @patch('patroni.api.RestApiServer')
    @patch('patroni.controller.ControllerPostgresql')
    @patch('patroni.controller.AgentClient')
    @patch('patroni.request.PatroniRequest')
    @patch('patroni.dcs.get_dcs')
    def test_constructs_without_local_postgresql(self, get_dcs, request, client, postgresql, api, ha,
                                                 ensure_access, ensure_name, apply_config, signals) -> None:
        config = ControllerConfigDict({
            'scope': 'cluster-a',
            'name': 'node-a',
            'ttl': 30,
            'loop_wait': 10,
            'retry_timeout': 10,
            'controller': {'socket': os.path.abspath('agent.sock')},
            'etcd3': {'host': 'etcd:2379'},
            'postgresql': {
                'scope': 'cluster-a',
                'name': 'node-a',
                'connect_address': 'postgres:5432',
            },
            'restapi': {},
        })
        get_dcs.return_value = Mock(mpp=Null())

        with patch('patroni.controller.peer_check'):
            controller = PatroniController(config, Mock())

        client.assert_called_once()
        postgresql.assert_called_once()
        self.assertEqual(client.return_value, controller.node)

    def test_grants_use_patroni_deadline(self) -> None:
        controller = object.__new__(PatroniController)
        controller.config = {
            'ttl': 30,
            'loop_wait': 10,
            'retry_timeout': 10,
            'watchdog': {'safety_margin': 5},
        }
        controller.node = Mock(
            controller_boot_id='controller-id',
            agent_boot_id='agent-id',
        )
        controller.node.watchdog.return_value = Mock(running=True)
        controller._authority_kind = None
        controller._authority_term = 0

        controller.grant_authority(AuthorityKind.LEADER)
        controller.grant_authority(AuthorityKind.LEADER)
        controller.grant_authority(AuthorityKind.FAILSAFE)

        grants = tuple(call[0][0] for call in controller.node.grant.call_args_list)
        self.assertEqual((1, 1, 2), tuple(grant.term for grant in grants))
        for grant in grants:
            self.assertAlmostEqual(25.0, grant.deadline - grant.issued_at)


class ControllerConfigDict(dict):

    dynamic_configuration = {}


if __name__ == '__main__':
    unittest.main()
