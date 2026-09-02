"""Agent-local replication and watchdog mechanics."""
from typing import Any, Callable, cast, Dict, Mapping, Optional, Tuple, TYPE_CHECKING

from patroni.collections import CaseInsensitiveSet
from patroni.dcs import Cluster, ClusterConfig, Leader, Member, Status, SyncState
from patroni.postgresql.misc import PostgresqlState

from .commands import SlotAction, SlotPlan, SyncAction, SyncCount, SyncPlan
from .models import SlotCapabilities, SlotContext, SlotSpec, SyncContext, SyncSnapshot, \
    SyncType, WatchdogMode, WatchdogReload, WatchdogSnapshot, WatchdogTiming

if TYPE_CHECKING:  # pragma: no cover
    from patroni.watchdog import Watchdog as _Watchdog

    from .node import NodeControl as _NodeControl


class PostgresReplication:
    """Keep host-local safety devices below ``NodeControl``."""

    def __init__(self, postgresql: Any, watchdog: '_Watchdog', timing_revision: int = 0,
                 watchdog_config: Optional[Callable[[], Mapping[str, Any]]] = None) -> None:
        self._postgresql = postgresql
        self._watchdog = watchdog
        self._timing_revision = timing_revision
        self._timing = WatchdogTiming(
            timing_revision,
            int(watchdog.config.ttl),
            int(watchdog.config.loop_wait),
            int(watchdog.config.safety_margin),
            WatchdogMode(watchdog.config.mode),
        )
        self._watchdog_config = watchdog_config or _empty_config

    def watchdog(self) -> WatchdogSnapshot:
        return WatchdogSnapshot(self._watchdog.is_running, self._watchdog.is_healthy)

    def activate_watchdog(self) -> bool:
        return self._watchdog.activate()

    def disable_watchdog(self) -> None:
        self._watchdog.disable()

    def keepalive_watchdog(self) -> None:
        self._watchdog.keepalive()

    def reload_watchdog(self, timing: WatchdogTiming) -> WatchdogReload:
        _watchdog_timing(timing)
        if timing.revision < self._timing_revision:
            raise ValueError('stale watchdog timing')
        if timing.revision == self._timing_revision:
            if timing != self._timing:
                raise ValueError('conflicting watchdog timing')
            return WatchdogReload.REPLAYED

        watchdog = dict(self._watchdog_config())
        driver = watchdog.get('driver')
        driver_config = {
            key: value for key, value in watchdog.items()
            if key not in ('mode', 'safety_margin', 'driver')
        }
        config = {
            'ttl': timing.ttl,
            'loop_wait': timing.loop_wait,
            'watchdog': {
                **driver_config,
                'mode': timing.mode.value,
                'safety_margin': timing.safety_margin,
                'driver': driver or 'default',
            },
        }
        self._watchdog.reload_config(config)
        self._timing = timing
        self._timing_revision = timing.revision
        return WatchdogReload.APPLIED

    def sync_state(self, context: SyncContext) -> SyncSnapshot:
        state = self._postgresql.sync_handler.current_state(_cluster(context))
        return SyncSnapshot(
            SyncType(state.sync_type),
            state.numsync,
            tuple(state.sync),
            tuple(state.sync_confirmed),
            tuple(state.active),
            self._postgresql.synchronous_standby_names(),
        )

    def apply_sync(self, plan: SyncPlan) -> None:
        if plan.action == SyncAction.REFRESH:
            value = self._postgresql.config.synchronous_standby_names
            self._postgresql.config.set_synchronous_standby_names(value)
            return

        members = CaseInsensitiveSet(plan.members)
        if plan.count_mode == SyncCount.DEFAULT:
            self._postgresql.sync_handler.set_synchronous_standby_names(members)
            return

        self._postgresql.sync_handler.set_synchronous_standby_names(members, plan.numsync)

    def slot_capabilities(self) -> SlotCapabilities:
        return SlotCapabilities(self._postgresql.name, self._postgresql.can_advance_slots)

    def apply_slots(self, plan: SlotPlan) -> Tuple[str, ...]:
        cluster, tags = _slot_cluster(plan.context)
        if plan.action == SlotAction.COPY:
            slots = {slot.name: _slot(slot) for slot in plan.slots}
            self._postgresql.slots_handler.apply_logical_slots(
                cluster, tags, list(plan.copy_slots), slots,
            )
            return ()

        slots = {slot.name: _slot(slot) for slot in plan.slots}
        result = self._postgresql.slots_handler.apply_replication_slots(cluster, tags, slots)
        return tuple(result)


class NodeWatchdog:
    """Preserve the HA watchdog interface through ``NodeControl``."""

    def __init__(self, node: '_NodeControl') -> None:
        self._node = node

    @property
    def is_running(self) -> bool:
        return self._node.watchdog().running

    @property
    def is_healthy(self) -> bool:
        return self._node.watchdog().healthy

    def activate(self) -> bool:
        return self._node.activate_watchdog()

    def disable(self) -> None:
        self._node.disable_watchdog()

    def keepalive(self) -> None:
        self._node.keepalive_watchdog()


def _cluster(context: SyncContext) -> Cluster:
    members: list[Member] = []
    for item in context.members:
        tags: Dict[str, Any] = {
            'nofailover': item.nofailover,
            'nosync': item.nosync,
            'sync_priority': item.sync_priority,
        }
        if item.replicatefrom:
            tags['replicatefrom'] = item.replicatefrom
        data = {
            'state': PostgresqlState.RUNNING if item.running else PostgresqlState.STOPPED,
            'tags': tags,
        }
        members.append(Member(-1, item.name, None, data))

    sync = SyncState(-1, context.leader, ','.join(context.voters) or None, context.quorum)
    return Cluster(None, None, None, Status.empty(), members, None, sync, None, None)


def _slot_cluster(context: SlotContext) -> Tuple[Cluster, Member]:
    members: list[Member] = []
    leader_member = None
    for item in context.members:
        tags: Dict[str, Any] = {
            'nofailover': item.tags.nofailover,
            'nostream': item.tags.nostream,
        }
        if item.tags.replicatefrom:
            tags['replicatefrom'] = item.tags.replicatefrom
        data: Dict[str, Any] = {
            'state': PostgresqlState.RUNNING if item.running else PostgresqlState.STOPPED,
            'tags': tags,
            'conn_kwargs': {'host': item.host, 'port': item.port, 'dbname': item.database},
        }
        if item.lsn is not None:
            data['xlog_location'] = item.lsn
        member = Member(-1, item.name, None, data)
        members.append(member)
        if item.name == context.leader:
            leader_member = member

    leader = Leader(-1, None, leader_member) if leader_member else None
    config = ClusterConfig(-1, {}, -1) if context.config_present else None
    status = Status(0, dict(context.status_slots), list(context.retain_slots), None)
    cluster = Cluster(None, config, leader, status, members, None, SyncState.empty(), None, None)
    local_data = {'tags': {
        'nofailover': context.local_tags.nofailover,
        'nostream': context.local_tags.nostream,
        'replicatefrom': context.local_tags.replicatefrom,
    }}
    return cluster, Member(-1, context.local_name, None, local_data)


def _slot(spec: SlotSpec) -> Dict[str, Any]:
    value: Dict[str, Any] = {'type': spec.kind.value}
    for name in ('database', 'plugin', 'lsn', 'expected_active', 'failover'):
        field = getattr(spec, name)
        if field is not None:
            value[name] = field
    return value


def _empty_config() -> Mapping[str, Any]:
    return {}


def _watchdog_timing(value: object) -> None:
    if not isinstance(value, WatchdogTiming) \
            or not isinstance(cast(object, value.mode), WatchdogMode):
        raise ValueError('invalid watchdog timing')
    for item in (value.revision, value.ttl, value.loop_wait, value.safety_margin):
        if not isinstance(cast(object, item), int) or isinstance(cast(object, item), bool):
            raise ValueError('invalid watchdog timing')
    if value.revision < 0 or value.ttl <= 0 or value.loop_wait <= 0:
        raise ValueError('invalid watchdog timing')
