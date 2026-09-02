import unittest

from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from patroni.control import BootstrapState, CheckpointMode, CloneMode, CommandDriver, CommandKind, \
    CommandValue, DesiredRole, DivergencePolicy, DriverResult, InProcessNodeControl, LifecycleCommand, \
    ReloadMode, SlotAction, SlotContext, SlotKind, SlotMember, SlotPlan, SlotSpec, SlotTags, StopMode, \
    SyncAction, SyncContext, SyncCount, SyncMember, SyncPlan, WatchdogMode, WatchdogReload, WatchdogTiming
from patroni.control.commands import AgentCommands
from patroni.control.replication import PostgresReplication


class BlockingDriver(CommandDriver):

    def __init__(self) -> None:
        self.entered = Event()
        self.release = Event()

    def run(self, command, events, cancelled):
        self.entered.set()
        self.release.wait(1)
        return DriverResult(CommandValue.TRUE, None, None, ())

    def cancel(self) -> None:
        self.release.set()


def command():
    return LifecycleCommand(
        str(uuid4()), CommandKind.STOP, DesiredRole.UNCHANGED, None,
        StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None, BootstrapState.IDLE,
        None, None,
    )


def watchdog():
    value = Mock()
    value.config = SimpleNamespace(
        ttl=30,
        loop_wait=10,
        safety_margin=5,
        mode='automatic',
    )
    value.is_running = True
    value.is_healthy = True
    return value


class TestPostgresReplication(unittest.TestCase):

    def setUp(self) -> None:
        self.postgresql = Mock()
        self.postgresql.name = 'node-a'
        self.postgresql.can_advance_slots = True
        self.watchdog = watchdog()
        self.replication = PostgresReplication(self.postgresql, self.watchdog)

    def test_watchdog_bypasses_busy_worker(self) -> None:
        driver = BlockingDriver()
        commands = AgentCommands(driver)
        node = InProcessNodeControl(
            str(uuid4()), Mock(), lambda: 1.0, commands, replication=self.replication,
        )
        commands.submit(command())
        self.assertTrue(driver.entered.wait(1))

        node.keepalive_watchdog()

        self.watchdog.keepalive.assert_called_once_with()
        driver.release.set()
        commands.close()

    def test_sync_uses_bounded_context(self) -> None:
        state = SimpleNamespace(
            sync_type='priority',
            numsync=1,
            sync=('node-b',),
            sync_confirmed=('node-b',),
            active=('node-b',),
        )
        self.postgresql.sync_handler.current_state.return_value = state
        self.postgresql.synchronous_standby_names.return_value = 'FIRST 1 (node-b)'
        context = SyncContext(
            'node-a',
            ('node-b',),
            0,
            (SyncMember('node-b', True, False, False, None, 1),),
        )

        snapshot = self.replication.sync_state(context)

        cluster = self.postgresql.sync_handler.current_state.call_args.args[0]
        self.assertEqual('node-a', cluster.sync.leader)
        self.assertEqual(['node-b'], [member.name for member in cluster.members])
        self.assertEqual(('node-b',), snapshot.confirmed)
        self.assertEqual('FIRST 1 (node-b)', snapshot.configured)

    def test_sync_preserves_explicit_count(self) -> None:
        plan = SyncPlan(SyncAction.SET, ('node-b',), SyncCount.EXPLICIT, 0)

        self.replication.apply_sync(plan)

        call = self.postgresql.sync_handler.set_synchronous_standby_names.call_args
        self.assertEqual(0, call.args[1])

    def test_slot_plan_contains_no_credentials(self) -> None:
        context = SlotContext(
            'node-a',
            True,
            'node-b',
            (
                SlotMember(
                    'node-b', '127.0.0.2', '5432', 'postgres', True, 10,
                    SlotTags(False, False, None),
                ),
            ),
            (('logical_a', 10),),
            (),
            SlotTags(False, False, None),
        )
        spec = SlotSpec('logical_a', SlotKind.LOGICAL, 'postgres', 'test', 10, None, False)
        plan = SlotPlan(SlotAction.APPLY, context, (spec,), ())
        self.postgresql.slots_handler.apply_replication_slots.return_value = ['logical_a']

        output = self.replication.apply_slots(plan)

        cluster, tags, slots = self.postgresql.slots_handler.apply_replication_slots.call_args.args
        self.assertEqual(('logical_a',), output)
        self.assertEqual('node-a', tags.name)
        self.assertEqual(
            {'host': '127.0.0.2', 'port': '5432', 'dbname': 'postgres'},
            cluster.leader.conn_kwargs(),
        )
        self.assertNotIn('password', repr(plan))
        self.assertEqual('logical', slots['logical_a']['type'])

    def test_watchdog_timing_is_ordered(self) -> None:
        self.replication = PostgresReplication(
            self.postgresql,
            self.watchdog,
            watchdog_config=lambda: {'driver': 'testing', 'device': '/dev/watchdog-test'},
        )
        timing = WatchdogTiming(1, 40, 10, 5, WatchdogMode.REQUIRED)

        self.assertEqual(WatchdogReload.APPLIED, self.replication.reload_watchdog(timing))
        self.assertEqual(WatchdogReload.REPLAYED, self.replication.reload_watchdog(timing))
        config = self.watchdog.reload_config.call_args.args[0]
        self.assertEqual(40, config['ttl'])
        self.assertEqual('testing', config['watchdog']['driver'])
        self.assertEqual('/dev/watchdog-test', config['watchdog']['device'])

        with self.assertRaises(ValueError):
            self.replication.reload_watchdog(timing._replace(revision=0))
