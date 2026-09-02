"""Filtered dynamic configuration below the agent boundary."""
import hashlib
import json
import math

from typing import Any, cast, Dict, List, Mapping, TYPE_CHECKING

from patroni import global_config
from patroni.postgresql.misc import PostgresqlRole

from .models import ConfigApply, DynamicConfigPlan

if TYPE_CHECKING:  # pragma: no cover
    from patroni.config import Config
    from patroni.postgresql import Postgresql

AGENT_DYNAMIC_KEYS = frozenset((
    'ignore_slots',
    'loop_wait',
    'member_slots_ttl',
    'pause',
    'permanent_replication_slots',
    'permanent_slots',
    'postgresql',
    'retry_timeout',
    'slots',
    'standby_cluster',
    'synchronous_mode',
    'synchronous_mode_strict',
    'synchronous_node_count',
    'ttl',
    'maximum_lag_on_syncnode',
))
LOCAL_POSTGRES_KEYS = frozenset((
    'authentication',
    'config_dir',
    'connect_address',
    'data_dir',
    'listen',
    'pgpass',
    'proxy_address',
))
MAX_CONFIG_DEPTH = 16
MAX_CONFIG_ITEMS = 8192
MAX_CONFIG_TEXT = 64 * 1024
MAX_CONFIG_INTEGER = (1 << 63) - 1
FINGERPRINT_LENGTH = 64


def config_plan(revision: int, dynamic: Mapping[str, Any]) -> DynamicConfigPlan:
    """Build one bounded, agent-relevant DCS configuration plan."""
    _revision(revision)
    document: Dict[str, Any] = {}
    for key, value in dynamic.items():
        if key not in AGENT_DYNAMIC_KEYS:
            continue
        if key == 'postgresql':
            value = _postgresql(value)
        document[key] = _copy_value(value, 0)
    fingerprint = _fingerprint(document)

    return DynamicConfigPlan(revision, fingerprint, document)


def _postgresql(value: object) -> Dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError('dynamic PostgreSQL configuration is invalid')

    values = cast(Mapping[object, object], value)
    result: Dict[str, object] = {}
    for key, item in values.items():
        if not isinstance(key, str):
            raise ValueError('dynamic PostgreSQL key is invalid')
        if key in LOCAL_POSTGRES_KEYS:
            continue
        result[key] = item

    return result


class AgentConfigManager:
    """Apply ordered DCS configuration through Patroni's existing config path."""

    def __init__(self, config: 'Config', postgresql: 'Postgresql') -> None:
        self._config = config
        self._postgresql = postgresql
        self._revision = -1
        self._fingerprint = ''

    def apply(self, plan: DynamicConfigPlan) -> ConfigApply:
        document = _validate_plan(plan)
        if plan.revision < self._revision:
            raise ValueError('dynamic configuration revision is stale')
        if plan.revision == self._revision:
            if plan.fingerprint != self._fingerprint:
                raise ValueError('dynamic configuration revision conflicts')

            return ConfigApply.REPLAYED

        self._config.set_dynamic_configuration(document)
        global_config.update(None, self._config.dynamic_configuration)
        self._reload()
        self.save_cache()
        self._revision = plan.revision
        self._fingerprint = plan.fingerprint

        return ConfigApply.APPLIED

    def reload_local(self) -> None:
        """Apply an already-loaded local configuration."""
        self._reload()

    def save_cache(self) -> None:
        """Keep an uninitialized PGDATA empty, matching monolithic Patroni."""
        if self._postgresql.role == PostgresqlRole.UNINITIALIZED:
            return

        self._config.save_cache()

    def _reload(self) -> None:
        effective = self._config.build_effective_postgresql_configuration(self._postgresql.role)
        self._postgresql.reload_config(effective)


def _validate_plan(plan: object) -> Dict[str, Any]:
    if not isinstance(plan, DynamicConfigPlan):
        raise ValueError('dynamic configuration plan is invalid')
    _revision(plan.revision)
    fingerprint = cast(object, plan.fingerprint)
    document_value = cast(object, plan.document)
    if not isinstance(fingerprint, str) or len(fingerprint) != FINGERPRINT_LENGTH:
        raise ValueError('dynamic configuration fingerprint is invalid')
    if not isinstance(document_value, dict):
        raise ValueError('dynamic configuration document is invalid')
    raw_document = cast(Dict[object, object], document_value)
    if any(key not in AGENT_DYNAMIC_KEYS for key in raw_document):
        raise ValueError('dynamic configuration key is not agent-owned')

    document = cast(Dict[str, Any], _copy_value(raw_document, 0))
    if _fingerprint(document) != fingerprint:
        raise ValueError('dynamic configuration fingerprint mismatch')

    return document


def _revision(value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError('dynamic configuration revision is invalid')
    if value > MAX_CONFIG_INTEGER:
        raise ValueError('dynamic configuration revision is invalid')


def _copy_value(value: object, depth: int) -> Any:
    if depth > MAX_CONFIG_DEPTH:
        raise ValueError('dynamic configuration exceeds depth limit')
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) > MAX_CONFIG_INTEGER:
            raise ValueError('dynamic configuration integer exceeds limit')
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError('dynamic configuration number is invalid')
        return value
    if isinstance(value, str):
        if len(value.encode('utf-8')) > MAX_CONFIG_TEXT or '\x00' in value:
            raise ValueError('dynamic configuration text exceeds limit')
        return value
    if isinstance(value, list):
        values = cast(List[object], value)
        if len(values) > MAX_CONFIG_ITEMS:
            raise ValueError('dynamic configuration list exceeds limit')
        return [_copy_value(item, depth + 1) for item in values]
    if isinstance(value, dict):
        values_map = cast(Dict[object, object], value)
        if len(values_map) > MAX_CONFIG_ITEMS:
            raise ValueError('dynamic configuration map exceeds limit')
        copied: Dict[str, Any] = {}
        for key, item in values_map.items():
            if not isinstance(key, str) or not key or '\x00' in key:
                raise ValueError('dynamic configuration key is invalid')
            copied[key] = _copy_value(item, depth + 1)
        return copied

    raise ValueError('dynamic configuration value is invalid')


def _fingerprint(document: Dict[str, Any]) -> str:
    payload = json.dumps(
        document, allow_nan=False, ensure_ascii=True, separators=(',', ':'), sort_keys=True,
    ).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()
