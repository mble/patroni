"""DCS-owning Patroni controller process."""
import logging
import os
import time

from typing import Any, cast, Dict, FrozenSet, List, Mapping, NamedTuple, Optional, Tuple, TYPE_CHECKING
from uuid import uuid4

from patroni import global_config
from patroni.__main__ import Patroni
from patroni.control import AgentClient, AuthorityGrant, AuthorityKind, Freshness, NodeControl, ObservationContext, \
    PolicyMode, PostgresRole, PostgresState, SnapshotDetail, Timing, WatchdogMode, WatchdogTiming
from patroni.control.config import config_plan
from patroni.control.unix import peer_check
from patroni.daemon import abstract_main, AbstractPatroniDaemon, get_base_arg_parser
from patroni.exceptions import PatroniFatalException
from patroni.postgresql.misc import PostgresqlRole, PostgresqlState
from patroni.postgresql.mpp import AbstractMPP
from patroni.site import ClusterSite
from patroni.utils import uri

if TYPE_CHECKING:  # pragma: no cover
    from patroni.config import Config
    from patroni.log import PatroniLogger

logger = logging.getLogger(__name__)

CONTROLLER_DCS = 'etcd3'
OTHER_DCS_SECTIONS = frozenset((
    'consul',
    'etcd',
    'exhibitor',
    'kubernetes',
    'raft',
    'zookeeper',
))
CONTROLLER_POSTGRES_KEYS = frozenset((
    'connect_address',
    'database',
    'name',
    'proxy_address',
    'scope',
))
CONTROLLER_BOOTSTRAP_KEYS = frozenset(('dcs',))
CONTROLLER_WATCHDOG_KEYS = frozenset(('mode', 'safety_margin'))
DEFAULT_THREAD_POOL_SIZE = 5


class ControllerConfig(NamedTuple):
    """Validated controller transport configuration."""

    socket: str
    timeout: float
    peer_uid: Optional[int]
    peer_gid: Optional[int]


def _controller_config(config: Mapping[str, object]) -> ControllerConfig:
    if not config.get(CONTROLLER_DCS):
        raise PatroniFatalException('controller configuration requires etcd3')
    configured = sorted(section for section in OTHER_DCS_SECTIONS if config.get(section))
    if configured:
        raise PatroniFatalException(
            'controller configuration contains unsupported DCS section: {0}'.format(', '.join(configured)),
        )
    if config.get('citus'):
        raise PatroniFatalException('controller mode does not support Citus')

    raw = config.get('controller')
    if not isinstance(raw, Mapping):
        raise PatroniFatalException('controller configuration requires an agent socket')
    values = cast(Mapping[str, object], raw)
    socket = values.get('socket')
    timeout = values.get('timeout', 5.0)
    if not isinstance(socket, str) or not os.path.isabs(socket):
        raise PatroniFatalException('controller agent socket is invalid')
    if not isinstance(timeout, (float, int)) or isinstance(timeout, bool) or timeout <= 0:
        raise PatroniFatalException('controller agent timeout is invalid')

    raw_postgres: object = config.get('postgresql')
    if raw_postgres is None:
        raw_postgres = cast(Mapping[str, object], {})
    if not isinstance(raw_postgres, Mapping):
        raise PatroniFatalException('controller PostgreSQL metadata is invalid')
    postgres = cast(Mapping[str, object], raw_postgres)
    private = sorted(key for key in postgres if key not in CONTROLLER_POSTGRES_KEYS)
    if private:
        raise PatroniFatalException(
            'controller configuration contains agent-only PostgreSQL key: {0}'.format(', '.join(private)),
        )

    _owned_keys(config.get('bootstrap'), CONTROLLER_BOOTSTRAP_KEYS, 'bootstrap')
    _owned_keys(config.get('watchdog'), CONTROLLER_WATCHDOG_KEYS, 'watchdog')

    return ControllerConfig(
        socket,
        float(timeout),
        _peer_id(values.get('peer_uid')),
        _peer_id(values.get('peer_gid')),
    )


def _peer_id(value: object) -> Optional[int]:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PatroniFatalException('controller peer identity is invalid')

    return value


def _owned_keys(value: object, allowed: FrozenSet[str], section: str) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise PatroniFatalException('controller {0} configuration is invalid'.format(section))
    values = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in values):
        raise PatroniFatalException('controller {0} configuration is invalid'.format(section))
    keys = cast(List[str], list(values))
    unexpected = sorted(key for key in keys if key not in allowed)
    if unexpected:
        raise PatroniFatalException(
            'controller configuration contains agent-only {0} key: {1}'.format(
                section, ', '.join(unexpected),
            ),
        )


class ControllerPostgresql:
    """Expose HA metadata without local PostgreSQL access."""

    def __init__(self, config: Mapping[str, object], mpp: AbstractMPP,
                 node: NodeControl) -> None:
        self.name = _text(config.get('name'), 'PostgreSQL name')
        self.scope = _text(config.get('scope'), 'PostgreSQL scope')
        address = _text(config.get('connect_address'), 'PostgreSQL connect address')
        database = _text(config.get('database', 'postgres'), 'PostgreSQL database')
        self.connection_string = uri('postgres', address, database)
        proxy_address = config.get('proxy_address')
        self.proxy_url = uri('postgres', proxy_address, database) \
            if isinstance(proxy_address, str) and proxy_address else None
        self._node = node
        self._role_hint: Optional[PostgresqlRole] = None
        self._state_hint: Optional[PostgresqlState] = None
        self._state_since = time.monotonic()
        self._last_state: Optional[PostgresqlState] = None
        self.mpp_handler = mpp.get_handler_impl(cast(Any, self))

    @property
    def role(self) -> PostgresqlRole:
        """Return the controller hint or current agent role."""
        if self._role_hint is not None:
            return self._role_hint

        role = self._snapshot().desired_role
        return _role(role)

    @property
    def state(self) -> PostgresqlState:
        """Return the controller hint or current agent state."""
        if self._state_hint is not None:
            return self._state_hint

        return self._state()

    def set_role(self, role: PostgresqlRole) -> None:
        if not isinstance(cast(object, role), PostgresqlRole):
            raise ValueError('invalid PostgreSQL role')
        self._role_hint = role

    def set_state(self, state: PostgresqlState) -> None:
        if not isinstance(cast(object, state), PostgresqlState):
            raise ValueError('invalid PostgreSQL state')
        self._state_hint = state
        self._record_state(state)

    def is_healthy(self) -> bool:
        return self._state() in (
            PostgresqlState.RUNNING,
            PostgresqlState.STARTING,
            PostgresqlState.RESTARTING,
        )

    def check_for_startup(self) -> bool:
        return self._state() in (
            PostgresqlState.STARTING,
            PostgresqlState.RESTARTING,
        )

    def time_in_state(self) -> float:
        self._state()
        return time.monotonic() - self._state_since

    def get_primary_timeline(self) -> int:
        return self._node.timeline_wal().timeline

    def get_history(self, timeline: int) -> Tuple[Tuple[object, ...], ...]:
        return self._node.timeline_history(timeline)

    def latest_checkpoint_locations(self) -> Tuple[Optional[int], Optional[int]]:
        return self._node.checkpoint_locations()

    def reset_cluster_info_state(self, cluster: object,
                                 patroni: Optional[object] = None) -> None:
        self._role_hint = None
        self._state_hint = None
        self._node.invalidate()

    def schedule_sanity_checks_after_pause(self) -> None:
        self._node.invalidate()

    def _snapshot(self):
        return self._node.snapshot(
            SnapshotDetail.BASIC, Freshness.FRESH, ObservationContext(None),
        )

    def _state(self) -> PostgresqlState:
        state = _state(self._snapshot().postgres_state)
        self._record_state(state)
        return state

    def _record_state(self, state: PostgresqlState) -> None:
        if state != self._last_state:
            self._last_state = state
            self._state_since = time.monotonic()


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or '\x00' in value:
        raise PatroniFatalException('{0} is invalid'.format(field))

    return value


def _role(role: PostgresRole) -> PostgresqlRole:
    if role == PostgresRole.UNKNOWN:
        return PostgresqlRole.UNINITIALIZED

    return PostgresqlRole(role.value)


def _state(state: PostgresState) -> PostgresqlState:
    if state == PostgresState.UNKNOWN:
        return PostgresqlState.STOPPED

    return PostgresqlState(state.value)


class PatroniController(Patroni):
    """Run Patroni policy against a separate PostgreSQL agent."""

    def __init__(self, config: 'Config', patroni_logger: 'PatroniLogger') -> None:
        from patroni import thread_pool
        from patroni.api import RestApiServer
        from patroni.dcs import get_dcs
        from patroni.ha import Ha
        from patroni.request import PatroniRequest
        from patroni.version import __version__

        local_config = getattr(config, 'local_configuration', config)
        control = _controller_config(cast(Mapping[str, object], local_config))
        try:
            thread_pool_size = max(DEFAULT_THREAD_POOL_SIZE, int(
                config.get('thread_pool_size', DEFAULT_THREAD_POOL_SIZE),
            ))
        except Exception as exc:
            logger.warning('Invalid thread_pool_size: %r', exc)
            thread_pool_size = DEFAULT_THREAD_POOL_SIZE
        thread_pool.configure_global_pool(thread_pool_size)

        AbstractPatroniDaemon.__init__(self, config, patroni_logger)
        ClusterSite.__init__(self, config.get('site'))
        self.version = __version__
        self.dcs = get_dcs(self.config)
        self.request = PatroniRequest(self.config, True)
        cluster = self.ensure_dcs_access()
        self.ensure_unique_name(cluster)
        self._watchdog_revision = 0
        self.apply_dynamic_configuration(cluster)
        global_config.update(None, self.config.dynamic_configuration)

        controller_boot_id = str(uuid4())
        self.node = AgentClient(
            control.socket,
            controller_boot_id,
            control.timeout,
            peer_check(control.peer_uid, control.peer_gid),
        )
        self._config_revision = _config_revision(cluster)
        self._push_config()
        postgresql = cast(Mapping[str, object], self.config['postgresql'])
        self.postgresql = ControllerPostgresql(postgresql, self.dcs.mpp, self.node)
        self.api = RestApiServer(self, self.config['restapi'])
        self.ha = Ha(self)
        self._tags = self._get_tags()
        self.next_run = time.time()
        self.scheduled_restart: Dict[str, Any] = {}
        self._authority_kind = self.node.authority_kind
        self._authority_term = self.node.authority_term
        self._authority_deadline = 0.0
        self._reload_watchdog()

    def reload_config(self, sighup: bool = False, local: Optional[bool] = False) -> None:
        try:
            AbstractPatroniDaemon.reload_config(self, sighup, local)
            if local:
                self._tags = self._get_tags()
                self.request.reload_config(self.config)
            received_new_cert = sighup and self.api.reload_local_certificate()
            if local or received_new_cert:
                self.api.reload_config(self.config['restapi'])
            self._reload_watchdog()
            self.dcs.reload_config(self.config)
        except Exception:
            logger.exception('Failed to reload controller configuration')

    def _reload_watchdog(self) -> None:
        if not hasattr(self, 'node'):
            return

        raw = cast(object, self.config.get('watchdog') or {})
        watchdog = cast(Dict[str, Any], raw) if isinstance(raw, dict) else {}
        raw_mode = watchdog.get('mode', WatchdogMode.AUTOMATIC.value)
        if raw_mode is False:
            mode = WatchdogMode.OFF
        elif str(raw_mode).lower() in ('require', WatchdogMode.REQUIRED.value):
            mode = WatchdogMode.REQUIRED
        elif str(raw_mode).lower() in ('auto', WatchdogMode.AUTOMATIC.value):
            mode = WatchdogMode.AUTOMATIC
        else:
            mode = WatchdogMode.OFF

        self._watchdog_revision += 1
        self.node.reload_watchdog(WatchdogTiming(
            self._watchdog_revision,
            int(self.config['ttl']),
            int(self.config['loop_wait']),
            int(watchdog.get('safety_margin', 5)),
            mode,
        ))

    def _run_cycle(self) -> None:
        logger.info(self.ha.run_cycle())
        cluster = self.dcs.cluster
        if cluster and cluster.config and cluster.config.data \
                and self.config.set_dynamic_configuration(cluster.config):
            self._config_revision = _config_revision(cluster)
            self._push_config()
            self.reload_config()
        self.schedule_next_run()

    def _push_config(self) -> None:
        plan = config_plan(self._config_revision, self.config.dynamic_configuration)
        self.node.configure(plan)

    def set_agent_policy(self, mode: PolicyMode) -> None:
        self.node.policy(mode)

    def grant_authority(self, kind: AuthorityKind) -> None:
        if kind != self._authority_kind:
            self._authority_term += 1
            self._authority_kind = kind
        now = time.monotonic()
        ttl = float(self.config['ttl'])
        loop_wait = float(self.config['loop_wait'])
        retry_timeout = float(self.config['retry_timeout'])
        watchdog_timeout = self._watchdog_timeout(ttl)
        lifetime = min(ttl, watchdog_timeout) if watchdog_timeout is not None else ttl
        self._authority_deadline = now + lifetime
        self.node.grant(AuthorityGrant(
            kind,
            self.node.controller_boot_id,
            self.node.agent_boot_id,
            self._authority_term,
            1,
            now,
            now + lifetime,
            Timing(ttl, loop_wait, retry_timeout, watchdog_timeout),
        ))

    def agent_metrics(self) -> Dict[str, object]:
        now = time.monotonic()
        major, minor = self.node.protocol_version
        telemetry = self.node.telemetry()
        return {
            'connected': float(self.node.connected),
            'snapshot_age': max(0.0, now - self.node.last_snapshot_at) if self.node.last_snapshot_at else 0.0,
            'authority_remaining': max(0.0, self._authority_deadline - now),
            'protocol_major': float(major),
            'protocol_minor': float(minor),
            'active_command': telemetry.active_command.value if telemetry.active_command else 'none',
            'active_phase': telemetry.active_phase.value if telemetry.active_phase else 'none',
            'fence_count': float(telemetry.fence_count),
            'fence_reason': telemetry.fence_reason.value,
            'config_revision': float(telemetry.config_revision),
            'config_fingerprint': telemetry.config_fingerprint or 'none',
        }

    def _watchdog_timeout(self, ttl: float) -> Optional[float]:
        snapshot = self.node.watchdog()
        if not snapshot.running:
            return None
        raw = cast(object, self.config.get('watchdog') or {})
        watchdog = cast(Dict[str, Any], raw) if isinstance(raw, dict) else {}
        safety_margin = int(watchdog.get('safety_margin', 5))
        return ttl / 2 if safety_margin == -1 else ttl - safety_margin


def main() -> None:
    args = get_base_arg_parser().parse_args()
    abstract_main(PatroniController, args.configfile)


def _config_revision(cluster: object) -> int:
    config = getattr(cluster, 'config', None)
    revision = getattr(config, 'modify_version', 0)
    if isinstance(revision, str) and revision.isascii() and revision.isdigit():
        revision = int(revision)
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise PatroniFatalException('DCS configuration revision is invalid')

    return revision


if __name__ == '__main__':  # pragma: no cover
    main()
