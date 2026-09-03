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
        monitor.bind(lambda: SafetyAction.FENCE, fence.set)
        monitor.start()

        self.assertFalse(blocked_worker.is_set())
        self.assertTrue(fence.wait(1))
        monitor.close()

    def test_fence_is_not_repeated(self) -> None:
        called = []
        fence = Event()

        def run_fence() -> None:
            called.append(1)
            fence.set()

        monitor = AuthorityMonitor(0.01)
        monitor.bind(lambda: SafetyAction.FENCE, run_fence)
        monitor.start()
        self.assertTrue(fence.wait(1))
        monitor.wake()
        monitor.close()

        self.assertEqual([1], called)

    def test_bind_is_one_time(self) -> None:
        monitor = AuthorityMonitor()
        monitor.bind(lambda: SafetyAction.NONE, lambda: None)

        with self.assertRaises(RuntimeError):
            monitor.bind(lambda: SafetyAction.NONE, lambda: None)

    def test_schedule_suppresses_polling(self) -> None:
        checked = Event()
        calls = []
        monitor = AuthorityMonitor(0.01)

        def guard() -> SafetyAction:
            calls.append(1)
            checked.set()
            return SafetyAction.NONE

        monitor.bind(guard, lambda: None, lambda: None)
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

        monitor.bind(guard, fenced.set, schedule)
        monitor.start()

        self.assertTrue(fenced.wait(1))
        monitor.close()
