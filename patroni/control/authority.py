"""Independent agent authority deadline monitoring."""
import logging

from threading import Event, RLock, Thread
from typing import Callable, Optional

from .models import SafetyAction

logger = logging.getLogger(__name__)

DEFAULT_CHECK_INTERVAL = 0.1


class AuthorityMonitor:
    """Check authority outside the PostgreSQL command worker."""

    def __init__(self, interval: float = DEFAULT_CHECK_INTERVAL) -> None:
        if interval <= 0:
            raise ValueError('authority interval must be positive')

        self._interval = interval
        self._guard: Optional[Callable[[], SafetyAction]] = None
        self._fence: Optional[Callable[[], None]] = None
        self._lock = RLock()
        self._wake = Event()
        self._closed = Event()
        self._thread: Optional[Thread] = None
        self._fencing = False

    def bind(self, guard: Callable[[], SafetyAction], fence: Callable[[], None]) -> None:
        """Install the transport-owned safety callbacks once."""
        with self._lock:
            if self._guard is not None:
                raise RuntimeError('authority monitor is already bound')
            self._guard = guard
            self._fence = fence
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
        while not self._closed.is_set():
            self._wake.wait(self._interval)
            self._wake.clear()
            self._check()

    def _check(self) -> None:
        with self._lock:
            guard = self._guard
            fence = self._fence
        if guard is None or fence is None:
            return

        try:
            action = guard()
            if action != SafetyAction.FENCE:
                self._fencing = False
                return
            if self._fencing:
                return

            self._fencing = True
            fence()
        except Exception:
            self._fencing = False
            logger.exception('Authority monitor failed')
