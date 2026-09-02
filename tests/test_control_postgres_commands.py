import os
import time
import unittest

from threading import Event, Thread
from unittest.mock import Mock
from uuid import uuid4

from patroni.control import BootstrapState, CallbackKind, CloneMode, CommandKind, DesiredRole, DivergencePolicy
from patroni.control.commands import CheckpointMode, CommandValue, DriverResult, EventChannel, EventKind, \
    FollowTarget, LifecycleCommand, RecoveryTarget, ReloadMode, SlotMode, StopMode, TargetKind
from patroni.control.postgres_commands import PostgresCommandDriver
from patroni.postgresql.misc import PostgresqlRole
from patroni.postgresql.postmaster import PostmasterProcess


def command(kind, role=DesiredRole.UNCHANGED, timeout=1.0,
            stop_mode=StopMode.FAST, checkpoint=CheckpointMode.DEFAULT, target=None,
            reload_mode=ReloadMode.RESTART, recovery_target=None,
            clone_mode=CloneMode.CONFIGURED, divergence=DivergencePolicy.NONE,
            callback=None, bootstrap_state=BootstrapState.IDLE):
    events = (EventKind.SAFEPOINT, EventKind.BEFORE_SHUTDOWN, EventKind.SHUTDOWN)
    return LifecycleCommand(
        str(uuid4()), kind, role, timeout, stop_mode, checkpoint, events, target, reload_mode,
        recovery_target, clone_mode, divergence, callback, bootstrap_state,
    )


def next_event(channel, sequence):
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        events = channel.events(sequence)
        if events:
            return events[0]
        time.sleep(0.01)

    raise AssertionError('event was not published')


class TestPostgresCommandDriver(unittest.TestCase):

    def setUp(self) -> None:
        self.postgresql = Mock()
        self.driver = PostgresCommandDriver(self.postgresql)
        self.cancelled = Event()

    def test_stop_publishes_ordered_events(self) -> None:
        def stop(*args, **kwargs):
            kwargs['on_safepoint']()
            kwargs['before_shutdown']()
            kwargs['on_shutdown'](20, 10)
            return True

        self.postgresql.stop.side_effect = stop
        request = command(CommandKind.STOP)
        channel = EventChannel(request.command_id)
        result = []
        thread = Thread(target=lambda: result.append(self.driver.run(request, channel, self.cancelled)))
        thread.start()

        for sequence, kind in enumerate((EventKind.SAFEPOINT, EventKind.BEFORE_SHUTDOWN, EventKind.SHUTDOWN), 1):
            event = next_event(channel, sequence - 1)
            self.assertEqual(kind, event.kind)
            channel.ack(sequence)

        thread.join(1)
        self.assertEqual([DriverResult(CommandValue.TRUE, 20, 10)], result)
        self.postgresql.stop.assert_called_once()
        self.assertEqual('fast', self.postgresql.stop.call_args.args[0])
        self.assertNotIn('checkpoint', self.postgresql.stop.call_args.kwargs)

    def test_lost_event_does_not_block_stop(self) -> None:
        def stop(*args, **kwargs):
            kwargs['on_shutdown'](20, 10)
            return True

        self.postgresql.stop.side_effect = stop
        request = command(CommandKind.STOP, timeout=0)

        value = self.driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.TRUE, 20, 10), value)

    def test_start_preserves_pending_result(self) -> None:
        self.postgresql.start.return_value = None
        request = command(CommandKind.START, DesiredRole.PRIMARY)

        value = self.driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.PENDING, None, None), value)
        self.assertEqual(PostgresqlRole.PRIMARY, self.postgresql.start.call_args.kwargs['role'])

    def test_cancel_reaches_postgresql(self) -> None:
        self.driver.cancel()

        self.postgresql.cancellable.cancel.assert_called_once_with()

    def test_cancel_terminates_started_postmaster(self) -> None:
        entered = Event()
        release = Event()
        postmaster = PostmasterProcess(os.getpid())

        def start(timeout, task, **kwargs):
            with task:
                task.complete(postmaster)
            entered.set()
            release.wait(1)
            return False

        self.postgresql.start.side_effect = start
        self.postgresql.cancellable.cancel.side_effect = release.set
        request = command(CommandKind.START)
        thread = Thread(target=self.driver.run, args=(request, EventChannel(request.command_id), self.cancelled))
        thread.start()
        self.assertTrue(entered.wait(1))

        self.driver.cancel()
        thread.join(1)

        self.postgresql.terminate_starting_postmaster.assert_called_once_with(postmaster)

    def test_fence_is_immediate_without_checkpoint(self) -> None:
        self.postgresql.stop.return_value = True
        request = command(CommandKind.FENCE)

        value = self.driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.TRUE, None, None), value)
        self.postgresql.stop.assert_called_once_with('immediate', checkpoint=False, stop_timeout=1)

    def test_follow_uses_credential_free_target(self) -> None:
        self.postgresql.follow.return_value = True
        target = FollowTarget(TargetKind.MEMBER, 'leader', '127.0.0.1', '5432', 'postgres', None, SlotMode.USE)
        request = command(CommandKind.FOLLOW, DesiredRole.REPLICA, target=target, reload_mode=ReloadMode.RELOAD)

        value = self.driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.TRUE, None, None), value)
        member = self.postgresql.follow.call_args.args[0]
        self.assertEqual({'host': '127.0.0.1', 'port': '5432', 'dbname': 'postgres'},
                         member.data['conn_kwargs'])
        self.assertNotIn('password', repr(member))
        self.assertTrue(self.postgresql.follow.call_args.kwargs['do_reload'])

    def test_rewind_uses_typed_policy(self) -> None:
        recovery = Mock()
        recovery.execute.return_value = True
        driver = PostgresCommandDriver(self.postgresql, recovery)
        target = RecoveryTarget(
            TargetKind.MEMBER, 'leader', '127.0.0.1', '5432', 'postgres', None,
            SlotMode.USE, 'primary', True,
        )
        request = command(
            CommandKind.REWIND,
            DesiredRole.REPLICA,
            recovery_target=target,
            divergence=DivergencePolicy.REWIND,
        )

        result = driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.TRUE, None, None), result)
        recovery.execute.assert_called_once_with(target)
        self.assertNotIn('password', repr(request))

    def test_failed_clone_cleans_pgdata(self) -> None:
        recovery = Mock()
        recovery.clone.return_value = False
        driver = PostgresCommandDriver(self.postgresql, recovery)
        request = command(CommandKind.CLONE, DesiredRole.REPLICA)

        result = driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.FALSE, None, None), result)
        recovery.remove_data.assert_called_once_with()

    def test_callback_is_allowlisted(self) -> None:
        recovery = Mock()
        driver = PostgresCommandDriver(self.postgresql, recovery)
        request = command(CommandKind.CALLBACK, callback=CallbackKind.START)

        result = driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual(DriverResult(CommandValue.TRUE, None, None), result)
        recovery.callback.assert_called_once_with(CallbackKind.START)

    def test_bootstrap_sets_running_state_first(self) -> None:
        calls = []
        recovery = Mock()
        recovery.set_bootstrapping.side_effect = lambda state: calls.append(('state', state))
        recovery.bootstrap.side_effect = lambda: calls.append(('bootstrap', None)) or True
        driver = PostgresCommandDriver(self.postgresql, recovery)
        request = command(
            CommandKind.BOOTSTRAP,
            DesiredRole.PRIMARY,
            bootstrap_state=BootstrapState.RUNNING,
        )

        driver.run(request, EventChannel(request.command_id), self.cancelled)

        self.assertEqual([('state', BootstrapState.RUNNING), ('bootstrap', None)], calls)


if __name__ == '__main__':
    unittest.main()
