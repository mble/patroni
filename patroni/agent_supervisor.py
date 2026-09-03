"""PID-1 supervisor for the Patroni agent container."""
import argparse
import errno
import logging
import os
import signal
import subprocess
import sys

from types import FrameType
from typing import Any, Dict, List, Optional

from patroni.version import __version__

logger = logging.getLogger(__name__)

FORWARDED_SIGNALS = (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
SIGNAL_EXIT_OFFSET = 128
CONFIG_ENV = 'PATRONI_CONFIGURATION'


class AgentSupervisor:
    """Reap children and exit when the agent daemon exits."""

    def __init__(self, command: List[str]) -> None:
        if not command:
            raise ValueError('agent command is empty')
        self._command = command
        self._child: Optional[subprocess.Popen[bytes]] = None
        self._handlers: Dict[int, Any] = {}
        self._pending_signal: Optional[int] = None

    def run(self) -> int:
        if os.name != 'posix':
            raise RuntimeError('agent supervisor requires POSIX')
        if os.getpid() != 1:
            logger.warning('Agent supervisor is not PID 1')

        self._install_handlers()
        try:
            self._child = subprocess.Popen(self._command, close_fds=True)
            self._forward_pending()
            return self._reap(self._child.pid)
        finally:
            self._restore_handlers()

    def _forward(self, signum: int, frame: Optional[FrameType]) -> None:
        child = self._child
        if child is None:
            self._pending_signal = signum
            return
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            pass

    def _forward_pending(self) -> None:
        signum = self._pending_signal
        if signum is None:
            return
        self._pending_signal = None
        self._forward(signum, None)

    def _install_handlers(self) -> None:
        for signum in FORWARDED_SIGNALS:
            self._handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, self._forward)

    def _restore_handlers(self) -> None:
        for signum, handler in self._handlers.items():
            signal.signal(signum, handler)
        self._handlers.clear()

    @staticmethod
    def _reap(agent_pid: int) -> int:
        while True:
            try:
                pid, status = os.waitpid(-1, 0)
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                raise
            if pid != agent_pid:
                continue
            if os.WIFEXITED(status):
                return os.WEXITSTATUS(status)
            if os.WIFSIGNALED(status):
                return SIGNAL_EXIT_OFFSET + os.WTERMSIG(status)
            raise RuntimeError('agent entered an unexpected wait state')


def main() -> None:
    # Keep PID 1 independent of Patroni's configuration dependency tree.
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {0}'.format(__version__))
    parser.add_argument(
        'configfile', nargs='?', default='',
        help='configuration file; {0} is also supported'.format(CONFIG_ENV),
    )
    args = parser.parse_args()
    command = [sys.executable, '-m', 'patroni.agent']
    if args.configfile:
        command.append(args.configfile)
    sys.exit(AgentSupervisor(command).run())


if __name__ == '__main__':  # pragma: no cover
    main()
