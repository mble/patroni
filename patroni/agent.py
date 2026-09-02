"""DCS-free PostgreSQL agent daemon."""
import logging
import time

from threading import Event
from typing import Any, cast, Protocol, TYPE_CHECKING
from uuid import uuid4

from patroni import global_config
from patroni.control import AgentCommands, BootstrapState, CheckpointMode, CloneMode, \
    CommandKind, CommandState, DesiredRole, DivergencePolicy, InProcessNodeControl, \
    LifecycleCommand, PolicyMode, ReloadMode, StopMode, SubmitState
from patroni.control.authority import AuthorityMonitor
from patroni.control.postgres import LocalPostgresObserver
from patroni.control.postgres_commands import PostgresCommandDriver
from patroni.control.recovery import PostgresRecovery
from patroni.control.replication import PostgresReplication
from patroni.daemon import abstract_main, AbstractPatroniDaemon, get_base_arg_parser
from patroni.exceptions import PatroniFatalException
from patroni.postgresql import Postgresql
from patroni.postgresql.mpp import get_mpp
from patroni.watchdog import Watchdog

if TYPE_CHECKING:  # pragma: no cover
    from patroni.config import Config
    from patroni.log import PatroniLogger

logger = logging.getLogger(__name__)

DCS_SECTIONS = frozenset((
    'consul',
    'etcd',
    'etcd3',
    'exhibitor',
    'kubernetes',
    'raft',
    'zookeeper',
))
SHUTDOWN_POLL_SECONDS = 0.05


class _ConfigView(Protocol):

    def get(self, key: str, default: Any = None) -> Any:
        ...


class PatroniAgent(AbstractPatroniDaemon):
    """Own PostgreSQL and host-local mechanics without a DCS client."""

    def __init__(self, config: 'Config', patroni_logger: 'PatroniLogger') -> None:
        _reject_dcs(config)
        super().__init__(config, patroni_logger)

        self.agent_boot_id = str(uuid4())
        self._wake = Event()
        self._policy = PolicyMode.ACTIVE
        self.authority = AuthorityMonitor()

        global_config.update(None, config.dynamic_configuration)
        self.watchdog = Watchdog(config)
        self.postgresql = Postgresql(config['postgresql'], get_mpp(config))
        recovery = PostgresRecovery(self.postgresql, self._wake.set, config.get('bootstrap'))
        replication = PostgresReplication(
            self.postgresql,
            self.watchdog,
            watchdog_config=lambda: config.get('watchdog') or {},
        )
        commands = AgentCommands(PostgresCommandDriver(self.postgresql, recovery, replication))
        self.node = InProcessNodeControl(
            self.agent_boot_id,
            LocalPostgresObserver(self.postgresql),
            time.monotonic,
            commands,
            recovery,
            replication,
        )

    def set_policy(self, mode: PolicyMode) -> None:
        """Retain active or paused shutdown semantics."""
        if not isinstance(cast(object, mode), PolicyMode):
            raise ValueError('invalid agent policy')
        self._policy = mode

    def run(self) -> None:
        self.authority.start()
        super().run()

    def _run_cycle(self) -> None:
        self._wake.wait(1)
        self._wake.clear()

    def _shutdown(self) -> None:
        self.authority.close()
        if self._policy == PolicyMode.ACTIVE:
            self._stop_postgres()
        self.node.disable_watchdog()
        self.node.close()

    def _stop_postgres(self) -> None:
        timeout = float(self.config.get('retry_timeout', 10))
        deadline = time.monotonic() + timeout
        active = self.node.active_command()
        if active is not None:
            self.node.command_cancel(active.request.command_id)
        while self.node.active_command() is not None and time.monotonic() < deadline:
            time.sleep(SHUTDOWN_POLL_SECONDS)
        if self.node.active_command() is not None:
            logger.error('Agent command did not cancel before shutdown')
            return

        command = LifecycleCommand(
            str(uuid4()),
            CommandKind.STOP,
            DesiredRole.UNCHANGED,
            timeout,
            StopMode.FAST,
            CheckpointMode.DEFAULT,
            (),
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
        submission = self.node.submit(command)
        if submission.state not in (SubmitState.ACCEPTED, SubmitState.REPLAYED):
            logger.error('Agent shutdown command was rejected')
            return

        remaining = max(0.0, deadline - time.monotonic())
        result = self.node.command_wait(command.command_id, remaining)
        if result is None or result.state != CommandState.SUCCEEDED:
            logger.error('PostgreSQL did not stop before agent shutdown')


def _reject_dcs(config: _ConfigView) -> None:
    configured = sorted(section for section in DCS_SECTIONS if config.get(section))
    if configured:
        raise PatroniFatalException(
            'agent configuration contains DCS section: {0}'.format(', '.join(configured)),
        )


def main() -> None:
    args = get_base_arg_parser().parse_args()
    abstract_main(PatroniAgent, args.configfile)


if __name__ == '__main__':  # pragma: no cover
    main()
