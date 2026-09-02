"""DCS-free PostgreSQL agent daemon."""
import logging
import time

from threading import Event
from typing import Any, cast, Mapping, NamedTuple, Optional, Protocol, TYPE_CHECKING
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
from patroni.control.rpc import AgentRpc
from patroni.control.unix import DEFAULT_MAX_WORKERS, DEFAULT_SOCKET_MODE, DEFAULT_TIMEOUT, peer_check, UnixServer
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


class _ControlConfig(NamedTuple):
    path: str
    timeout: float
    max_workers: int
    socket_mode: int
    peer_uid: Optional[int]
    peer_gid: Optional[int]


class _ConfigView(Protocol):

    def get(self, key: str, default: Any = None) -> Any:
        ...


class PatroniAgent(AbstractPatroniDaemon):
    """Own PostgreSQL and host-local mechanics without a DCS client."""

    def __init__(self, config: 'Config', patroni_logger: 'PatroniLogger') -> None:
        _reject_dcs(config)
        control_config = _control_config(config)
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
        self._rpc = AgentRpc(
            self.node, self.agent_boot_id, time.monotonic, self.authority, self.set_policy,
        )
        self._server = UnixServer(
            control_config.path,
            self._rpc.handle,
            peer_check(control_config.peer_uid, control_config.peer_gid),
            control_config.timeout,
            control_config.max_workers,
            control_config.socket_mode,
        )

    def set_policy(self, mode: PolicyMode) -> None:
        """Retain active or paused shutdown semantics."""
        if not isinstance(cast(object, mode), PolicyMode):
            raise ValueError('invalid agent policy')
        self._policy = mode

    def run(self) -> None:
        self._server.start()
        self.authority.start()
        super().run()

    def _run_cycle(self) -> None:
        self._wake.wait(1)
        self._wake.clear()

    def _shutdown(self) -> None:
        self._server.close()
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


def _control_config(config: _ConfigView) -> _ControlConfig:
    raw = config.get('agent')
    if not isinstance(raw, Mapping):
        raise PatroniFatalException('agent configuration requires a control socket')

    values = cast(Mapping[str, object], raw)
    path = values.get('socket')
    timeout = values.get('timeout', DEFAULT_TIMEOUT)
    max_workers = values.get('max_workers', DEFAULT_MAX_WORKERS)
    socket_mode = values.get('socket_mode', DEFAULT_SOCKET_MODE)
    peer_uid = _peer_id(values.get('peer_uid'))
    peer_gid = _peer_id(values.get('peer_gid'))

    if not isinstance(path, str) or not path:
        raise PatroniFatalException('agent control socket is invalid')
    if not isinstance(timeout, (float, int)) or isinstance(timeout, bool) or timeout <= 0:
        raise PatroniFatalException('agent control timeout is invalid')
    if not isinstance(max_workers, int) or isinstance(max_workers, bool) or max_workers < 1:
        raise PatroniFatalException('agent control worker limit is invalid')
    if not isinstance(socket_mode, int) or isinstance(socket_mode, bool):
        raise PatroniFatalException('agent control socket mode is invalid')
    return _ControlConfig(
        path, float(timeout), max_workers, socket_mode, peer_uid, peer_gid,
    )


def _peer_id(value: object) -> Optional[int]:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PatroniFatalException('agent peer identity is invalid')

    return value


def main() -> None:
    args = get_base_arg_parser().parse_args()
    abstract_main(PatroniAgent, args.configfile)


if __name__ == '__main__':  # pragma: no cover
    main()
