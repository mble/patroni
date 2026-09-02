import unittest

from unittest.mock import Mock

from patroni.control import CallbackKind, RecoveryTarget, SlotMode, TargetKind
from patroni.control.recovery import PostgresRecovery


class TestPostgresRecovery(unittest.TestCase):

    def setUp(self) -> None:
        self.postgresql = Mock()
        self.recovery = PostgresRecovery(
            self.postgresql,
            lambda: None,
            {'dcs': {'password': 'controller-secret'}, 'method': 'initdb'},
        )

    def test_bootstrap_excludes_dcs_configuration(self) -> None:
        self.postgresql.bootstrap.bootstrap.return_value = True

        self.assertTrue(self.recovery.bootstrap())

        config = self.postgresql.bootstrap.bootstrap.call_args.args[0]
        self.assertEqual({'method': 'initdb'}, config)
        self.assertNotIn('controller-secret', repr(config))

    def test_rewind_target_contains_no_credentials(self) -> None:
        source = []
        self.recovery._rewind.rewind_or_reinitialize_needed_and_possible = Mock(
            side_effect=lambda member: source.append(member) or True,
        )
        target = RecoveryTarget(
            TargetKind.MEMBER, 'leader', '127.0.0.1', '5432', 'postgres', None,
            SlotMode.USE, 'primary', True,
        )

        self.assertTrue(self.recovery.needed(target))

        self.assertEqual('127.0.0.1', source[0].conn_kwargs()['host'])
        self.assertNotIn('password', repr(source[0]))

    def test_callback_maps_only_allowlisted_action(self) -> None:
        self.recovery.callback(CallbackKind.START)

        action = self.postgresql.call_nowait.call_args.args[0]
        self.assertEqual('on_start', action.value)


if __name__ == '__main__':
    unittest.main()
