import time
import unittest

from threading import Event

from patroni.control import SafetyAction
from patroni.control.authority import AuthorityMonitor


class TestAuthorityMonitor(unittest.TestCase):

    def test_fence_runs_on_independent_thread(self) -> None:
        fence = Event()
        blocked_worker = Event()
        monitor = AuthorityMonitor(0.01)

        def run_fence() -> bool:
            fence.set()
            return True

        monitor.bind(lambda: SafetyAction.FENCE, run_fence)
        monitor.start()

        self.assertFalse(blocked_worker.is_set())
        self.assertTrue(fence.wait(1))
        monitor.close()

    def test_fence_is_not_repeated(self) -> None:
        called = []
        fence = Event()

        def run_fence() -> bool:
            called.append(1)
            fence.set()
            return True

        monitor = AuthorityMonitor(0.01)
        monitor.bind(lambda: SafetyAction.FENCE, run_fence)
        monitor.start()
        self.assertTrue(fence.wait(1))
        monitor.wake()
        monitor.close()

        self.assertEqual([1], called)

    def test_failed_fence_is_retried(self) -> None:
        calls = []
        retried = Event()

        def run_fence() -> bool:
            calls.append(1)
            if len(calls) == 2:
                retried.set()

            return len(calls) > 1

        monitor = AuthorityMonitor(0.01)
        monitor.bind(lambda: SafetyAction.FENCE, run_fence)
        monitor.start()

        self.assertTrue(retried.wait(1))
        monitor.close()

        self.assertEqual([1, 1], calls)

    def test_failed_fence_ignores_later_schedule(self) -> None:
        retried = Event()
        calls = []

        def run_fence() -> bool:
            calls.append(1)
            if len(calls) == 2:
                retried.set()

            return len(calls) > 1

        monitor = AuthorityMonitor(0.01)
        monitor.bind(lambda: SafetyAction.FENCE, run_fence, lambda: 10.0)
        monitor.start()

        self.assertTrue(retried.wait(1))
        monitor.close()

    def test_bind_is_one_time(self) -> None:
        monitor = AuthorityMonitor()
        monitor.bind(lambda: SafetyAction.NONE, lambda: True)

        with self.assertRaises(RuntimeError):
            monitor.bind(lambda: SafetyAction.NONE, lambda: True)

    def test_schedule_suppresses_polling(self) -> None:
        checked = Event()
        calls = []
        monitor = AuthorityMonitor(0.01)

        def guard() -> SafetyAction:
            calls.append(1)
            checked.set()
            return SafetyAction.NONE

        monitor.bind(guard, lambda: True, lambda: None)
        monitor.start()
        self.assertTrue(checked.wait(1))
        time.sleep(0.03)
        monitor.close()

        self.assertEqual([1], calls)

    def test_schedule_fences_at_deadline(self) -> None:
        fenced = Event()
        deadline = time.monotonic() + 0.02
        monitor = AuthorityMonitor()

        def guard() -> SafetyAction:
            return SafetyAction.FENCE if time.monotonic() >= deadline else SafetyAction.NONE

        def schedule():
            return None if fenced.is_set() else max(0.0, deadline - time.monotonic())

        def run_fence() -> bool:
            fenced.set()
            return True

        monitor.bind(guard, run_fence, schedule)
        monitor.start()

        self.assertTrue(fenced.wait(1))
        monitor.close()

    def test_schedule_failure_does_not_stop_monitor(self) -> None:
        should_fence = Event()
        fenced = Event()
        scheduled = Event()
        schedule_calls = []
        monitor = AuthorityMonitor(0.01)

        def guard() -> SafetyAction:
            return SafetyAction.FENCE if should_fence.is_set() else SafetyAction.NONE

        def schedule():
            schedule_calls.append(1)
            scheduled.set()
            if len(schedule_calls) == 1:
                raise RuntimeError('schedule failed')

            return 0.01

        def run_fence() -> bool:
            fenced.set()
            return True

        monitor.bind(guard, run_fence, schedule)
        monitor.start()
        self.assertTrue(scheduled.wait(1))

        should_fence.set()
        monitor.wake()

        self.assertTrue(fenced.wait(1))
        monitor.close()
