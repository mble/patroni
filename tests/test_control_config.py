import unittest

from unittest.mock import Mock, patch

from patroni.control.config import AgentConfigManager, config_plan
from patroni.control.models import ConfigApply
from patroni.postgresql.misc import PostgresqlRole


class TestAgentConfigManager(unittest.TestCase):

    def setUp(self) -> None:
        self.config = Mock()
        self.config.set_dynamic_configuration.return_value = True
        self.config.dynamic_configuration = {'postgresql': {'use_slots': False}}
        self.config.build_effective_postgresql_configuration.return_value = {'retry_timeout': 10}
        self.postgresql = Mock(role=PostgresqlRole.REPLICA)
        self.manager = AgentConfigManager(self.config, self.postgresql)

    @patch('patroni.control.config.global_config.update')
    def test_applies_filtered_dynamic_plan(self, update) -> None:
        plan = config_plan(7, {
            'ttl': 30,
            'failsafe_mode': True,
            'postgresql': {'use_slots': False, 'parameters': {'max_connections': 200}},
            'standby_cluster': {'host': 'upstream'},
        })

        result = self.manager.apply(plan)

        self.assertEqual(ConfigApply.APPLIED, result)
        self.config.set_dynamic_configuration.assert_called_once_with({
            'ttl': 30,
            'postgresql': {'use_slots': False, 'parameters': {'max_connections': 200}},
            'standby_cluster': {'host': 'upstream'},
        })
        update.assert_called_once_with(None, self.config.dynamic_configuration)
        self.postgresql.reload_config.assert_called_once_with({'retry_timeout': 10})
        self.config.save_cache.assert_called_once_with()

    def test_replay_and_conflict_are_bounded(self) -> None:
        first = config_plan(7, {'postgresql': {'use_slots': False}})
        replay = config_plan(7, {'postgresql': {'use_slots': False}})
        conflict = config_plan(7, {'postgresql': {'use_slots': True}})

        self.assertEqual(ConfigApply.APPLIED, self.manager.apply(first))
        self.assertEqual(ConfigApply.REPLAYED, self.manager.apply(replay))
        with self.assertRaises(ValueError):
            self.manager.apply(conflict)

    def test_plan_excludes_local_postgresql_keys(self) -> None:
        plan = config_plan(7, {'postgresql': {
            'authentication': {'superuser': {'password': 'secret'}},
            'data_dir': '/private/pgdata',
            'listen': '127.0.0.1:5432',
            'parameters': {'max_connections': 200},
        }})

        self.assertEqual(
            {'postgresql': {'parameters': {'max_connections': 200}}},
            plan.document,
        )

    def test_rejects_stale_and_unbounded_values(self) -> None:
        self.manager.apply(config_plan(7, {}))

        with self.assertRaises(ValueError):
            self.manager.apply(config_plan(6, {}))
        with self.assertRaises(ValueError):
            config_plan(8, {'postgresql': {'parameters': {'bad': object()}}})

    def test_does_not_fill_uninitialized_data_dir(self) -> None:
        self.postgresql.role = PostgresqlRole.UNINITIALIZED

        self.manager.apply(config_plan(7, {'ttl': 30}))

        self.config.save_cache.assert_not_called()


if __name__ == '__main__':
    unittest.main()
