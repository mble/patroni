import unittest

from threading import Event, Thread
from uuid import uuid4

from patroni.control import BootstrapState, CallbackKind, CloneMode, CommandKind, CommandPhase, \
    CommandState, DesiredRole, DivergencePolicy, SlotAction, SlotContext, SlotPlan, SlotTags
from patroni.control.commands import AckState, AgentCommands, CheckpointMode, \
    CommandDriver, CommandResult, CommandValue, DriverResult, EventChannel, \
    EventKind, EventRecord, LifecycleCommand, ReloadMode, StopMode, SubmitState


class FakeDriver(CommandDriver):

    def __init__(self) -> None:
        self.entered = Event()
        self.release = Event()
        self.cancelled = Event()
        self.fence_entered = Event()
        self.fence_release = Event()
        self.fence_release.set()
        self.calls = []

    def run(self, command, events, cancelled):
        self.calls.append(command)
        self.entered.set()
        events.publish(EventKind.SAFEPOINT)
        events.publish(EventKind.SHUTDOWN, 20, 10)
        self.release.wait(1)
        if cancelled.is_set():
            return DriverResult(CommandValue.FALSE, 20, 10, ())

        return DriverResult(CommandValue.TRUE, 20, 10, ())

    def cancel(self) -> None:
        self.cancelled.set()
        self.release.set()

    def fence(self, timeout) -> bool:
        self.cancel()
        self.fence_entered.set()
        self.fence_release.wait(1)
        return True


class AckDriver(CommandDriver):

    def __init__(self) -> None:
        self.entered = Event()
        self.completed = Event()

    def run(self, command, events, cancelled):
        event = events.publish(EventKind.SAFEPOINT)
        self.entered.set()
        events.wait_ack(event.sequence, 5, cancelled)
        self.completed.set()
        return DriverResult(CommandValue.TRUE, None, None, ())

    def cancel(self) -> None:
        pass


def command(command_id=None, kind=CommandKind.STOP):
    return LifecycleCommand(
        command_id or str(uuid4()),
        kind,
        DesiredRole.UNCHANGED,
        None,
        StopMode.FAST,
        CheckpointMode.DEFAULT,
        (EventKind.SAFEPOINT, EventKind.BEFORE_SHUTDOWN, EventKind.SHUTDOWN),
        None,
        ReloadMode.RESTART,
        None,
        CloneMode.CONFIGURED,
        DivergencePolicy.NONE,
        None,
        BootstrapState.IDLE,
        None,
        None,
    )


class TestAgentCommands(unittest.TestCase):

    def setUp(self) -> None:
        self.driver = FakeDriver()
        self.commands = AgentCommands(self.driver)

    def tearDown(self) -> None:
        self.driver.release.set()
        self.commands.close()

    def test_submit_runs_asynchronously(self) -> None:
        request = command()

        submission = self.commands.submit(request)

        self.assertEqual(SubmitState.ACCEPTED, submission.state)
        self.assertTrue(self.driver.entered.wait(1))
        self.assertEqual(request.command_id, self.commands.active().request.command_id)
        self.assertEqual(CommandState.RUNNING, self.commands.status(request.command_id).state)

        self.driver.release.set()
        result = self.commands.wait(request.command_id, 1)

        self.assertEqual(CommandState.SUCCEEDED, result.state)
        self.assertEqual(CommandValue.TRUE, result.value)
        self.assertEqual(20, result.checkpoint_location)
        self.assertEqual(10, result.previous_location)
        self.assertIsNone(self.commands.active())

    def test_executor_reports_command_phases(self) -> None:
        phases = []
        self.commands.bind_phase(
            lambda command_id, phase: phases.append((command_id, phase)) or True,
        )
        request = command()
        self.commands.submit(request)
        self.assertTrue(self.driver.entered.wait(1))
        self.driver.release.set()
        self.commands.wait(request.command_id, 1)

        self.assertEqual([
            (request.command_id, CommandPhase.PREPARING),
            (request.command_id, CommandPhase.MUTATING),
            (request.command_id, CommandPhase.FINALIZING),
        ], phases)

    def test_busy_submission_is_rejected(self) -> None:
        first = command()
        second = command()
        self.commands.submit(first)
        self.assertTrue(self.driver.entered.wait(1))

        submission = self.commands.submit(second)

        self.assertEqual(SubmitState.BUSY, submission.state)
        self.assertIsNone(self.commands.status(second.command_id))

    def test_duplicate_submission_replays_status(self) -> None:
        request = command()
        first = self.commands.submit(request)
        self.assertTrue(self.driver.entered.wait(1))

        duplicate = self.commands.submit(request)

        self.assertEqual(SubmitState.REPLAYED, duplicate.state)
        self.assertEqual(first.result.request, duplicate.result.request)
        self.assertEqual(1, len(self.driver.calls))

    def test_conflicting_command_id_is_rejected(self) -> None:
        command_id = str(uuid4())
        self.commands.submit(command(command_id, CommandKind.STOP))
        self.assertTrue(self.driver.entered.wait(1))

        submission = self.commands.submit(command(command_id, CommandKind.START))

        self.assertEqual(SubmitState.CONFLICT, submission.state)
        self.assertEqual(1, len(self.driver.calls))

    def test_cancel_reaches_driver(self) -> None:
        request = command()
        self.commands.submit(request)
        self.assertTrue(self.driver.entered.wait(1))

        result = self.commands.cancel(request.command_id)

        self.assertTrue(self.driver.cancelled.is_set())
        self.assertEqual(CommandState.CANCELLED, result.state)

    def test_cancel_wakes_event_ack_wait(self) -> None:
        driver = AckDriver()
        commands = AgentCommands(driver)
        request = command()
        commands.submit(request)
        self.assertTrue(driver.entered.wait(1))

        commands.cancel(request.command_id)
        responsive = driver.completed.wait(0.2)
        commands.ack(request.command_id, 1)
        commands.close()

        self.assertTrue(responsive)

    def test_fence_preempts_active_command(self) -> None:
        request = command()
        self.commands.submit(request)
        self.assertTrue(self.driver.entered.wait(1))

        self.assertTrue(self.commands.fence(1))
        result = self.commands.wait(request.command_id, 1)

        self.assertEqual(CommandState.FENCED, result.state)
        self.assertIsNone(self.commands.active())

    def test_submit_waits_for_fence_completion(self) -> None:
        first = command()
        self.commands.submit(first)
        self.assertTrue(self.driver.entered.wait(1))
        self.driver.fence_release.clear()
        worker = Thread(target=self.commands.fence, args=(1,))
        worker.start()
        self.assertTrue(self.driver.fence_entered.wait(1))
        self.commands.wait(first.command_id, 1)

        submission = self.commands.submit(command())

        self.assertEqual(SubmitState.BUSY, submission.state)
        self.driver.fence_release.set()
        worker.join(1)

    def test_events_are_typed_and_replayable(self) -> None:
        request = command()
        self.commands.submit(request)
        self.assertTrue(self.driver.entered.wait(1))

        events = self.commands.events(request.command_id, 0)

        self.assertEqual((
            EventRecord(request.command_id, 1, EventKind.SAFEPOINT, None, None),
            EventRecord(request.command_id, 2, EventKind.SHUTDOWN, 20, 10),
        ), events)

        self.commands.ack(request.command_id, 1)
        self.assertEqual((events[1],), self.commands.events(request.command_id, 0))

    def test_unknown_status_is_absent(self) -> None:
        self.assertIsNone(self.commands.status(str(uuid4())))
        self.assertIsNone(self.commands.cancel(str(uuid4())))

    def test_invalid_command_is_rejected_before_start(self) -> None:
        request = LifecycleCommand(
            'not-a-uuid', CommandKind.STOP, DesiredRole.UNCHANGED, None,
            StopMode.FAST, CheckpointMode.DEFAULT, (), None, ReloadMode.RESTART,
            None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None, BootstrapState.IDLE,
            None,
            None,
        )

        with self.assertRaises(ValueError):
            self.commands.submit(request)

        self.assertFalse(self.driver.entered.is_set())

    def test_recovery_fields_are_kind_bound(self) -> None:
        request = command()._replace(callback=CallbackKind.START)

        with self.assertRaises(ValueError):
            self.commands.submit(request)

    def test_slot_status_accepts_member_names(self) -> None:
        context = SlotContext(
            'postgres-1', True, None, (), (('postgres-2', 10),), (),
            SlotTags(False, False, None),
        )
        plan = SlotPlan(SlotAction.APPLY, context, (), ())
        request = command(kind=CommandKind.APPLY_SLOTS)._replace(slot_plan=plan)

        submission = self.commands.submit(request)

        self.assertEqual(SubmitState.ACCEPTED, submission.state)

    def test_rewind_requires_target_and_policy(self) -> None:
        request = command(kind=CommandKind.REWIND)._replace(divergence=DivergencePolicy.REWIND)

        with self.assertRaises(ValueError):
            self.commands.submit(request)

    def test_result_repr_has_no_exception_detail(self) -> None:
        result = CommandResult(command(), CommandState.FAILED, CommandValue.NONE, None, None, ())

        self.assertNotIn('password', repr(result))

    def test_event_ack_delay_and_loss_are_explicit(self) -> None:
        channel = EventChannel(str(uuid4()))
        event = channel.publish(EventKind.SHUTDOWN, 20, 10)
        cancelled = Event()

        self.assertEqual(AckState.TIMED_OUT, channel.wait_ack(event.sequence, 0, cancelled))

        channel.ack(event.sequence)
        self.assertEqual(AckState.ACKED, channel.wait_ack(event.sequence, 0, cancelled))

        next_event = channel.publish(EventKind.BEFORE_SHUTDOWN)
        cancelled.set()
        self.assertEqual(AckState.CANCELLED, channel.wait_ack(next_event.sequence, 1, cancelled))

    def test_event_long_poll_returns_publish(self) -> None:
        channel = EventChannel(str(uuid4()))
        release = Event()

        def publish() -> None:
            release.wait(1)
            channel.publish(EventKind.SAFEPOINT)

        worker = Thread(target=publish)
        worker.start()
        release.set()

        events = channel.wait_events(0, 1)

        worker.join(1)
        self.assertEqual((EventKind.SAFEPOINT,), tuple(event.kind for event in events))


if __name__ == '__main__':
    unittest.main()
