"""Independent agent authority deadline monitoring."""
import logging

from enum import Enum
from threading import Event, RLock, Thread
from typing import Callable, Optional

from .models import SafetyAction

logger = logging.getLogger(__name__)

DEFAULT_CHECK_INTERVAL = 0.1


class _CheckResult(Enum):
    WAIT = 'wait'
    RETRY = 'retry'


class AuthorityMonitor:
    """Check authority outside the PostgreSQL command worker."""

    def __init__(self, interval: float = DEFAULT_CHECK_INTERVAL) -> None:
        if interval <= 0:
            raise ValueError('authority interval must be positive')

        self._interval = interval
        self._guard: Optional[Callable[[], SafetyAction]] = None
        self._fence: Optional[Callable[[], bool]] = None
        self._schedule: Optional[Callable[[], Optional[float]]] = None
        self._lock = RLock()
        self._wake = Event()
        self._closed = Event()
        self._thread: Optional[Thread] = None
        self._fencing = False

    def bind(self, guard: Callable[[], SafetyAction], fence: Callable[[], bool],
             schedule: Optional[Callable[[], Optional[float]]] = None) -> None:
        """Install the transport-owned safety callbacks once."""
        with self._lock:
            if self._guard is not None:
                raise RuntimeError('authority monitor is already bound')
            self._guard = guard
            self._fence = fence
            self._schedule = schedule
        self._wake.set()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            thread = Thread(target=self._run, name='agent-authority')
            thread.daemon = True
            self._thread = thread
            thread.start()

    def wake(self) -> None:
        self._wake.set()

    def close(self) -> None:
        self._closed.set()
        self._wake.set()
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(1)

    def _run(self) -> None:
        delay: Optional[float] = 0.0
        while not self._closed.is_set():
            self._wake.wait(delay)
            self._wake.clear()
            if self._closed.is_set():
                return
            result = self._check()
            delay = self._delay(result)

    def _delay(self, result: _CheckResult) -> Optional[float]:
        """Keep callback failures from disabling authority enforcement."""
        if result == _CheckResult.RETRY:
            return self._interval

        try:
            delay = self._next_delay()
        except Exception:
            logger.exception('Authority monitor scheduling failed')
            delay = self._interval

        return delay

    def _next_delay(self) -> Optional[float]:
        with self._lock:
            schedule = self._schedule
        if schedule is None:
            return self._interval

        delay = schedule()
        if delay is not None and delay < 0:
            raise ValueError('authority delay must not be negative')
        return delay

    def _check(self) -> _CheckResult:
        with self._lock:
            guard = self._guard
            fence = self._fence
        if guard is None or fence is None:
            return _CheckResult.WAIT

        try:
            action = guard()
            if action != SafetyAction.FENCE:
                self._fencing = False
                return _CheckResult.WAIT
            if self._fencing:
                return _CheckResult.WAIT

            self._fencing = True
            if fence():
                return _CheckResult.WAIT

            self._fencing = False
            return _CheckResult.RETRY
        except Exception:
            self._fencing = False
            logger.exception('Authority monitor failed')
            return _CheckResult.RETRY
