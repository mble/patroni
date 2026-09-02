"""Immutable controller-agent domain models."""
import datetime

from enum import Enum
from typing import Any, Dict, NamedTuple, Optional, Tuple


class AgentState(str, Enum):
    """Agent safety state."""

    IDLE = 'idle'
    BUSY = 'busy'
    FENCING = 'fencing'


class AuthorityKind(str, Enum):
    """Proof permitting primary behavior."""

    LEADER = 'leader'
    INITIALIZER = 'initializer'
    FAILSAFE = 'failsafe'


class AuthorityState(str, Enum):
    """Validity of the installed authority grant."""

    ABSENT = 'absent'
    CURRENT = 'current'
    EXPIRED = 'expired'


class CommandKind(str, Enum):
    """Local PostgreSQL operations."""

    START = 'start'
    STOP = 'stop'
    RESTART = 'restart'
    PROMOTE = 'promote'
    FOLLOW = 'follow'
    BOOTSTRAP = 'bootstrap'
    CLONE = 'clone'
    REWIND = 'rewind'
    CRASH_RECOVERY = 'crash_recovery'
    POST_BOOTSTRAP = 'post_bootstrap'
    REINITIALIZE = 'reinitialize'
    APPLY_CONFIG = 'apply_config'
    CALLBACK = 'callback'
    REMOVE_DATA = 'remove_data'
    MOVE_DATA = 'move_data'
    SET_BOOTSTRAP = 'set_bootstrap'
    RESET_RECOVERY = 'reset_recovery'
    CHECK_DIVERGENCE = 'check_divergence'
    ARCHIVE_WAL = 'archive_wal'
    APPLY_SYNC = 'apply_sync'
    APPLY_SLOTS = 'apply_slots'
    COPY_SLOTS = 'copy_slots'
    CHECKPOINT = 'checkpoint'
    FENCE = 'fence'


class CommandPhase(str, Enum):
    """Observable phase of one mutating command."""

    ACCEPTED = 'accepted'
    PREPARING = 'preparing'
    MUTATING = 'mutating'
    FINALIZING = 'finalizing'


class CommandState(str, Enum):
    """Command result state."""

    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    FENCED = 'fenced'


class DesiredRole(str, Enum):
    """Role requested by a command."""

    UNCHANGED = 'unchanged'
    PRIMARY = 'primary'
    REPLICA = 'replica'
    STANDBY_LEADER = 'standby_leader'


class PolicyMode(str, Enum):
    """Patroni policy mode."""

    ACTIVE = 'active'
    PAUSED = 'paused'


class ConfigApply(str, Enum):
    """Versioned dynamic configuration result."""

    APPLIED = 'applied'
    REPLAYED = 'replayed'


class FenceReason(str, Enum):
    """Last local fence trigger."""

    NONE = 'none'
    AUTHORITY = 'authority'
    COMMAND = 'command'
    EXPLICIT = 'explicit'


class PostgresRole(str, Enum):
    """PostgreSQL role relevant to primary safety."""

    UNKNOWN = 'unknown'
    UNINITIALIZED = 'uninitialized'
    PRIMARY = 'primary'
    MASTER = 'master'
    STANDBY_LEADER = 'standby_leader'
    REPLICA = 'replica'
    DEMOTED = 'demoted'
    PROMOTED = 'promoted'


class PostgresState(str, Enum):
    """PostgreSQL lifecycle state exposed across the boundary."""

    UNKNOWN = 'unknown'
    INITDB = 'initializing new cluster'
    INITDB_FAILED = 'initdb failed'
    CUSTOM_BOOTSTRAP = 'running custom bootstrap script'
    CUSTOM_BOOTSTRAP_FAILED = 'custom bootstrap failed'
    CREATING_REPLICA = 'creating replica'
    RUNNING = 'running'
    STARTING = 'starting'
    BOOTSTRAP_STARTING = 'starting after custom bootstrap'
    START_FAILED = 'start failed'
    RESTARTING = 'restarting'
    RESTART_FAILED = 'restart failed'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
    STOP_FAILED = 'stop failed'
    CRASHED = 'crashed'


class Freshness(str, Enum):
    """Snapshot collection policy."""

    CACHED = 'cached'
    FRESH = 'fresh'
    FRESH_RETRY = 'fresh_retry'


class QueryMode(str, Enum):
    """PostgreSQL query retry policy."""

    ONCE = 'once'
    RETRY = 'retry'


class SnapshotDetail(str, Enum):
    """Bounded observation set."""

    BASIC = 'basic'
    STATUS = 'status'


class ObservationFailure(str, Enum):
    """Non-sensitive observation failure."""

    NONE = 'none'
    QUERY_FAILED = 'query_failed'
    INCONSISTENT = 'inconsistent'
    AGENT_UNAVAILABLE = 'agent_unavailable'
    LIMIT_EXCEEDED = 'limit_exceeded'


class RecoverySnapshot(NamedTuple):
    """Agent-owned rewind state used by HA policy."""

    needed: bool
    executed: bool
    failed: bool
    checkpoint_after_promote: bool
    remove_on_divergence: bool
    bootstrapping: bool
    callback_called: bool


class ConfigChange(NamedTuple):
    """Recovery configuration action required by PostgreSQL."""

    change_required: bool
    restart_required: bool


class WatchdogSnapshot(NamedTuple):
    """Agent-local watchdog health."""

    running: bool
    healthy: bool


class WatchdogMode(str, Enum):
    """Normalized Patroni watchdog mode."""

    OFF = 'off'
    AUTOMATIC = 'automatic'
    REQUIRED = 'required'


class WatchdogReload(str, Enum):
    """Result of applying versioned watchdog timing."""

    APPLIED = 'applied'
    REPLAYED = 'replayed'


class WatchdogTiming(NamedTuple):
    """Controller-owned fencing timing."""

    revision: int
    ttl: int
    loop_wait: int
    safety_margin: int
    mode: WatchdogMode


class SyncType(str, Enum):
    """PostgreSQL synchronous replication mode."""

    OFF = 'off'
    PRIORITY = 'priority'
    QUORUM = 'quorum'


class SyncMember(NamedTuple):
    """Public member facts used for synchronous selection."""

    name: str
    running: bool
    nofailover: bool
    nosync: bool
    replicatefrom: Optional[str]
    sync_priority: int


class SyncContext(NamedTuple):
    """Bounded DCS facts used by the agent sync observer."""

    leader: Optional[str]
    voters: Tuple[str, ...]
    quorum: int
    members: Tuple[SyncMember, ...]


class SyncSnapshot(NamedTuple):
    """Agent-observed synchronous replication state."""

    sync_type: SyncType
    numsync: int
    sync: Tuple[str, ...]
    confirmed: Tuple[str, ...]
    active: Tuple[str, ...]
    configured: str


class SlotKind(str, Enum):
    """PostgreSQL replication slot type."""

    PHYSICAL = 'physical'
    LOGICAL = 'logical'


class SlotCapabilities(NamedTuple):
    """Local facts needed by controller slot policy."""

    name: str
    can_advance: bool


class SlotTags(NamedTuple):
    """Tags affecting replication-slot topology."""

    nofailover: bool
    nostream: bool
    replicatefrom: Optional[str]


class SlotMember(NamedTuple):
    """Credential-free member facts needed by slot mechanics."""

    name: str
    host: Optional[str]
    port: Optional[str]
    database: Optional[str]
    running: bool
    lsn: Optional[int]
    tags: SlotTags


class SlotContext(NamedTuple):
    """Bounded cluster facts needed to apply a slot plan."""

    local_name: str
    config_present: bool
    leader: Optional[str]
    members: Tuple[SlotMember, ...]
    status_slots: Tuple[Tuple[str, int], ...]
    retain_slots: Tuple[str, ...]
    local_tags: SlotTags


class SlotSpec(NamedTuple):
    """One desired replication slot."""

    name: str
    kind: SlotKind
    database: Optional[str]
    plugin: Optional[str]
    lsn: Optional[int]
    expected_active: Optional[bool]
    failover: Optional[bool]


class SafetyAction(str, Enum):
    """Side effect requested from the driver layer."""

    NONE = 'none'
    RUN = 'run'
    REJECT = 'reject'
    FENCE = 'fence'


class PendingRestart(NamedTuple):
    """One PostgreSQL parameter requiring restart."""

    name: str
    old_value: object
    new_value: object


class LocalPostgres(NamedTuple):
    """Atomic local state read by the PostgreSQL observer."""

    state: PostgresState
    observed_role: PostgresRole
    desired_role: PostgresRole
    system_identifier: str
    supports_multiple_sync: bool
    pending_restart: Tuple[PendingRestart, ...]


class ObservationContext(NamedTuple):
    """Public cluster facts needed to interpret local status."""

    leader_timeline: Optional[int]


class WalObservation(NamedTuple):
    """Coherent WAL fields from one status query."""

    location: Optional[int]
    received_location: Optional[int]
    replayed_location: Optional[int]
    replayed_timestamp: Optional[datetime.datetime]
    paused: Optional[bool]


class ReplicationConnection(NamedTuple):
    """Bounded public replication connection fields."""

    application_name: str
    client_addr: Optional[str]
    state: str
    sync_state: str
    sync_priority: int
    usename: str


class TimelineWal(NamedTuple):
    """Timeline and WAL positions from one PostgreSQL query."""

    timeline: int
    wal_position: int
    control_timeline: Optional[int]
    receive_lsn: Optional[int]
    replay_lsn: Optional[int]


class NodeSnapshot(NamedTuple):
    """Immutable coherent local node observation."""

    agent_boot_id: str
    sequence: int
    collected_at: float
    detail: SnapshotDetail
    observed_role: PostgresRole
    desired_role: PostgresRole
    postgres_state: PostgresState
    supports_multiple_sync: bool
    system_identifier: str
    server_version: int
    timeline: Optional[int]
    wal: WalObservation
    latest_end_lsn: Optional[int]
    replication_state: Optional[str]
    replication: Tuple[ReplicationConnection, ...]
    postmaster_start_time: Optional[datetime.datetime]
    pending_restart: Tuple[PendingRestart, ...]
    failure: ObservationFailure


class Timing(NamedTuple):
    """Effective Patroni timing values for one authority grant."""

    ttl: float
    loop_wait: float
    retry_timeout: float
    watchdog_timeout: Optional[float]


class AuthorityGrant(NamedTuple):
    """Bounded proof of controller authority."""

    kind: AuthorityKind
    controller_boot_id: str
    agent_boot_id: str
    term: int
    sequence: int
    issued_at: float
    deadline: float
    timing: Timing


class DynamicConfigPlan(NamedTuple):
    """Filtered DCS configuration for agent-local mechanics."""

    revision: int
    fingerprint: str
    document: Dict[str, Any]


class AgentTelemetry(NamedTuple):
    """Bounded agent observability state."""

    active_command: Optional[CommandKind]
    active_phase: Optional[CommandPhase]
    fence_count: int
    fence_reason: FenceReason
    config_revision: int
    config_fingerprint: str


class CommandRequest(NamedTuple):
    """Idempotent request for one local operation."""

    command_id: str
    controller_boot_id: str
    agent_boot_id: str
    sequence: int
    kind: CommandKind
    target_role: DesiredRole
    authority_term: int


class CommandStatus(NamedTuple):
    """Bounded command state retained for idempotency."""

    request: CommandRequest
    phase: CommandPhase
    state: CommandState


class CommandReceipt(NamedTuple):
    """Safety decision returned to the command service."""

    action: SafetyAction
    state: CommandState


class SafetySnapshot(NamedTuple):
    """Immutable state-machine observation."""

    agent_state: AgentState
    policy_mode: PolicyMode
    postgres_role: PostgresRole
    controller_boot_id: str
    agent_boot_id: str
    last_sequence: int
    authority_state: AuthorityState
    authority_kind: Optional[AuthorityKind]
    authority_term: int
    authority_deadline: float
    active_command_id: Optional[str]
    history_size: int
    connected: bool
