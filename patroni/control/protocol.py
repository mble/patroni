"""Bounded controller-agent wire protocol."""
import datetime
import json
import math
import struct

from enum import Enum
from socket import socket
from typing import Any, cast, Dict, List, Mapping, NamedTuple, Optional, Tuple, Type

import dateutil.parser

from .commands import AckState, BootstrapState, CallbackKind, CancelMode, CheckpointMode, \
    CloneMode, CommandResult, CommandSubmission, CommandValue, DivergencePolicy, EventKind, \
    EventRecord, FollowTarget, LifecycleCommand, RecoveryTarget, ReloadMode, SlotAction, \
    SlotMode, SlotPlan, StopMode, SubmitState, SyncAction, SyncCount, SyncPlan, TargetKind
from .models import AgentState, AgentTelemetry, AuthorityGrant, AuthorityKind, AuthorityState, CommandKind, \
    CommandPhase, CommandReceipt, CommandRequest, CommandState, CommandStatus, ConfigApply, ConfigChange, \
    DesiredRole, DynamicConfigPlan, FenceReason, Freshness, LocalPostgres, NodeSnapshot, ObservationContext, \
    ObservationFailure, PendingRestart, PolicyMode, PostgresRole, PostgresState, QueryMode, RecoverySnapshot, \
    ReplicationConnection, SafetyAction, SafetySnapshot, SlotCapabilities, SlotContext, SlotKind, SlotMember, \
    SlotSpec, SlotTags, SnapshotDetail, SyncContext, SyncMember, SyncSnapshot, SyncType, TimelineWal, Timing, \
    WalObservation, WatchdogMode, WatchdogReload, WatchdogSnapshot, WatchdogTiming

MAGIC = b'PAC1'
PROTOCOL_MAJOR = 1
PROTOCOL_MINOR = 1
HEADER = struct.Struct('!4sHHI')
MAX_FRAME_BYTES = 1024 * 1024
MAX_TEXT_BYTES = 64 * 1024
MAX_COLLECTION_ITEMS = 8192
MAX_DEPTH = 32
MAX_INTEGER = (1 << 63) - 1
RESERVED_KEYS = frozenset(('$datetime', '$enum', '$tuple', '$type'))


class Operation(str, Enum):
    """One bounded agent RPC."""

    HELLO = 'hello'
    SNAPSHOT = 'snapshot'
    INVALIDATE = 'invalidate'
    SUBMIT = 'submit'
    COMMAND_STATUS = 'command_status'
    ACTIVE_COMMAND = 'active_command'
    COMMAND_WAIT = 'command_wait'
    EVENTS = 'events'
    ACK = 'ack'
    CANCEL = 'cancel'
    CALL = 'call'
    GRANT = 'grant'
    POLICY = 'policy'
    FENCE = 'fence'
    CONFIGURE = 'configure'
    TELEMETRY = 'telemetry'


class NodeCall(str, Enum):
    """Allowlisted immediate NodeControl call."""

    IS_PRIMARY = 'is_primary'
    IS_RUNNING = 'is_running'
    IS_STARTING = 'is_starting'
    LAST_OPERATION = 'last_operation'
    TIMELINE_WAL = 'timeline_wal'
    REPLICATION_STATE = 'replication_state'
    RECEIVED_TIMELINE = 'received_timeline'
    REPLICA_TIMELINE = 'replica_timeline'
    CONTROL_TIMELINE = 'control_timeline'
    POSTMASTER_START = 'postmaster_start'
    SERVER_VERSION = 'server_version'
    SLOTS = 'slots'
    TIMELINE_HISTORY = 'timeline_history'
    CHECKPOINT_LOCATIONS = 'checkpoint_locations'
    RECOVERY = 'recovery'
    CAN_REWIND = 'can_rewind'
    REWIND_NEEDED = 'rewind_needed'
    ARCHIVE_READY = 'archive_ready'
    CAN_CLONE = 'can_clone'
    DATA_EMPTY = 'data_empty'
    CONTROLDATA = 'controldata'
    RESTORED = 'restored'
    RECOVERY_CONF_EXISTS = 'recovery_conf_exists'
    CHECK_RECOVERY_CONF = 'check_recovery_conf'
    CANCEL_RECOVERY = 'cancel_recovery'
    RESET_CANCEL = 'reset_cancel'
    WATCHDOG = 'watchdog'
    ACTIVATE_WATCHDOG = 'activate_watchdog'
    DISABLE_WATCHDOG = 'disable_watchdog'
    KEEPALIVE_WATCHDOG = 'keepalive_watchdog'
    RELOAD_WATCHDOG = 'reload_watchdog'
    SYNC_STATE = 'sync_state'
    SLOT_CAPABILITIES = 'slot_capabilities'


class Capability(str, Enum):
    """Negotiated controller-agent feature."""

    NODE_CONTROL = 'node_control'
    AUTHORITY_FENCING = 'authority_fencing'
    EVENT_ACK = 'event_ack'
    EVENT_LONG_POLL = 'event_long_poll'
    DYNAMIC_CONFIG = 'dynamic_config'
    TELEMETRY = 'telemetry'


class ErrorCode(str, Enum):
    """Public protocol failures."""

    BAD_REQUEST = 'bad_request'
    CONFLICT = 'conflict'
    FORBIDDEN = 'forbidden'
    INTERNAL = 'internal'
    STALE = 'stale'
    UNAVAILABLE = 'unavailable'
    VERSION = 'version'


class ProtocolError(ValueError):
    """A redacted wire-protocol failure."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class Request(NamedTuple):
    """Validated request envelope."""

    request_id: str
    operation: Operation
    controller_boot_id: str
    agent_boot_id: str
    sequence: int
    body: object


class Response(NamedTuple):
    """Validated response envelope."""

    request_id: str
    error: Optional[ErrorCode]
    body: object


class Hello(NamedTuple):
    """Negotiated agent identity and protocol features."""

    agent_boot_id: str
    protocol_major: int
    protocol_minor: int
    capabilities: Tuple[Capability, ...]
    authority_kind: Optional[AuthorityKind]
    authority_term: int


ENUM_TYPES: Tuple[Type[Enum], ...] = (
    AckState, AgentState, AuthorityKind, AuthorityState, BootstrapState, CallbackKind, CancelMode, Capability,
    CheckpointMode, CloneMode, CommandKind, CommandPhase, CommandState, CommandValue, DesiredRole, DivergencePolicy,
    ConfigApply, ErrorCode, EventKind, FenceReason, Freshness, NodeCall, ObservationFailure, Operation, PolicyMode,
    PostgresRole, PostgresState,
    QueryMode, ReloadMode, SafetyAction, SlotAction, SlotKind, SlotMode, SnapshotDetail, StopMode, SubmitState,
    SyncAction, SyncCount, SyncType, TargetKind, WatchdogMode, WatchdogReload,
)
RECORD_TYPES: Tuple[Type[Any], ...] = (
    AgentTelemetry, AuthorityGrant, CommandReceipt, CommandRequest, CommandResult, CommandStatus, CommandSubmission,
    ConfigChange, DynamicConfigPlan, EventRecord, FollowTarget, Hello, LifecycleCommand, LocalPostgres, NodeSnapshot,
    ObservationContext, PendingRestart, RecoveryTarget,
    RecoverySnapshot, ReplicationConnection, Request, Response, SafetySnapshot, SlotCapabilities, SlotContext,
    SlotMember, SlotPlan, SlotSpec, SlotTags, SyncContext, SyncMember, SyncPlan, SyncSnapshot, TimelineWal, Timing,
    WalObservation, WatchdogSnapshot, WatchdogTiming,
)
ENUM_REGISTRY = {item.__name__: item for item in ENUM_TYPES}
RECORD_REGISTRY = {item.__name__: item for item in RECORD_TYPES}


def pack(value: object) -> bytes:
    """Encode one bounded domain document."""
    document = _encode(value, 0)
    try:
        payload = json.dumps(
            document, allow_nan=False, ensure_ascii=True, separators=(',', ':'), sort_keys=True,
        ).encode('utf-8')
    except (TypeError, ValueError) as exc:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'document is not encodable') from exc
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'frame exceeds limit')
    return HEADER.pack(MAGIC, PROTOCOL_MAJOR, PROTOCOL_MINOR, len(payload)) + payload


def unpack(frame: bytes) -> object:
    """Decode one complete bounded frame."""
    if len(frame) < HEADER.size:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'frame header is incomplete')
    magic, major, minor, size = HEADER.unpack(frame[:HEADER.size])
    _header(magic, major, minor, size)
    payload = frame[HEADER.size:]
    if len(payload) != size:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'frame length mismatch')
    return _payload(payload)


def read_frame(stream: socket) -> object:
    """Read one frame without accepting partial input."""
    header = _read_exact(stream, HEADER.size)
    magic, major, minor, size = HEADER.unpack(header)
    _header(magic, major, minor, size)
    return _payload(_read_exact(stream, size))


def write_frame(stream: socket, value: object) -> None:
    """Write one complete frame."""
    stream.sendall(pack(value))


def _header(magic: bytes, major: int, minor: int, size: int) -> None:
    if magic != MAGIC:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'frame magic mismatch')
    if major != PROTOCOL_MAJOR or minor != PROTOCOL_MINOR:
        raise ProtocolError(ErrorCode.VERSION, 'protocol version mismatch')
    if size > MAX_FRAME_BYTES:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'frame exceeds limit')


def _payload(payload: bytes) -> object:
    try:
        document = json.loads(
            payload.decode('utf-8'),
            object_pairs_hook=_object,
            parse_constant=_constant,
        )
    except (TypeError, UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'payload is invalid JSON') from exc
    return _decode(document, 0)


def _read_exact(stream: socket, size: int) -> bytes:
    chunks: List[bytes] = []
    remaining = size
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'connection closed during frame')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def _object(pairs: List[Tuple[str, object]]) -> Dict[str, object]:
    if len(pairs) > MAX_COLLECTION_ITEMS:
        raise ValueError('object exceeds limit')
    value: Dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate object key')
        value[key] = item
    return value


def _constant(value: str) -> None:
    raise ValueError('non-finite number')


def _encode(value: object, depth: int) -> object:
    _depth(depth)
    if isinstance(value, Enum):
        name = value.__class__.__name__
        if name not in ENUM_REGISTRY or ENUM_REGISTRY[name] is not value.__class__:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'enum type is not allowed')
        return {'$enum': name, 'value': value.value}
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) > MAX_INTEGER:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'integer exceeds limit')
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'number is not finite')
        return value
    if isinstance(value, str):
        _text(value)
        return value
    if isinstance(value, datetime.datetime):
        return {'$datetime': value.isoformat()}
    if isinstance(value, tuple) and hasattr(cast(Any, value), '_fields'):
        raw_record = cast(Any, value)
        name = raw_record.__class__.__name__
        if name not in RECORD_REGISTRY or RECORD_REGISTRY[name] is not raw_record.__class__:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'record type is not allowed')
        fields = [_encode(item, depth + 1) for item in raw_record]
        return {'$type': name, 'fields': fields}
    if isinstance(value, tuple):
        raw_tuple = cast(Tuple[Any, ...], value)
        _items(raw_tuple)
        return {'$tuple': [_encode(item, depth + 1) for item in raw_tuple]}
    if isinstance(value, list):
        raw_list = cast(List[Any], value)
        _items(raw_list)
        return [_encode(item, depth + 1) for item in raw_list]
    if isinstance(value, Mapping):
        raw_mapping = cast(Mapping[Any, Any], value)
        _items(raw_mapping)
        document: Dict[str, object] = {}
        for key, item in raw_mapping.items():
            if not isinstance(key, str) or key in RESERVED_KEYS:
                raise ProtocolError(ErrorCode.BAD_REQUEST, 'object key is not allowed')
            _text(key)
            document[key] = _encode(item, depth + 1)
        return document
    raise ProtocolError(ErrorCode.BAD_REQUEST, 'value type is not allowed')


def _decode(value: object, depth: int) -> object:
    _depth(depth)
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, int) and not isinstance(value, bool) and abs(value) > MAX_INTEGER:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'integer exceeds limit')
        if isinstance(value, float) and not math.isfinite(value):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'number is not finite')
        if isinstance(value, str):
            _text(value)
        return value
    if isinstance(value, list):
        raw_list = cast(List[Any], value)
        _items(raw_list)
        return [_decode(item, depth + 1) for item in raw_list]
    if not isinstance(value, dict):
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'decoded value is invalid')

    data = cast(Dict[str, Any], value)
    tags = RESERVED_KEYS.intersection(data)
    if not tags:
        return {key: _decode(item, depth + 1) for key, item in data.items()}
    if len(tags) != 1:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'encoded tag is invalid')
    tag = next(iter(tags))
    if tag == '$datetime':
        _keys(data, ('$datetime',))
        raw = data[tag]
        if not isinstance(raw, str):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'datetime is invalid')
        try:
            return dateutil.parser.isoparse(raw)
        except ValueError as exc:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'datetime is invalid') from exc
    if tag == '$enum':
        _keys(data, ('$enum', 'value'))
        name = data[tag]
        raw = data['value']
        if not isinstance(name, str) or name not in ENUM_REGISTRY:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'enum is invalid')
        try:
            return ENUM_REGISTRY[name](raw)
        except (TypeError, ValueError) as exc:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'enum is invalid') from exc
    if tag == '$tuple':
        _keys(data, ('$tuple',))
        raw = data[tag]
        if not isinstance(raw, list):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'tuple is invalid')
        items = cast(List[Any], raw)
        _items(items)
        return tuple(_decode(item, depth + 1) for item in items)

    _keys(data, ('$type', 'fields'))
    name = data['$type']
    fields = data['fields']
    if not isinstance(name, str) or name not in RECORD_REGISTRY or not isinstance(fields, list):
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'record is invalid')
    record = RECORD_REGISTRY[name]
    items = cast(List[Any], fields)
    if len(items) != len(record._fields):
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'record field count is invalid')
    decoded = [_decode(item, depth + 1) for item in items]
    return cast(Any, record)(*decoded)


def _depth(depth: int) -> None:
    if depth > MAX_DEPTH:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'document nesting exceeds limit')


def _items(value: Any) -> None:
    if len(value) > MAX_COLLECTION_ITEMS:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'collection exceeds limit')


def _text(value: str) -> None:
    if len(value.encode('utf-8')) > MAX_TEXT_BYTES or '\x00' in value:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'text exceeds limit')


def _keys(value: Mapping[str, object], expected: Tuple[str, ...]) -> None:
    if set(value) != set(expected):
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'encoded fields are invalid')
