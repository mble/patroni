"""Immutable controller-agent domain models."""
from enum import Enum
from typing import NamedTuple, Optional


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
    REINITIALIZE = 'reinitialize'
    APPLY_CONFIG = 'apply_config'
    APPLY_SYNC = 'apply_sync'
    APPLY_SLOTS = 'apply_slots'
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


class PolicyMode(str, Enum):
    """Patroni policy mode."""

    ACTIVE = 'active'
    PAUSED = 'paused'


class PostgresRole(str, Enum):
    """PostgreSQL role relevant to primary safety."""

    UNKNOWN = 'unknown'
    STOPPED = 'stopped'
    PRIMARY = 'primary'
    REPLICA = 'replica'


class SafetyAction(str, Enum):
    """Side effect requested from the driver layer."""

    NONE = 'none'
    RUN = 'run'
    REJECT = 'reject'
    FENCE = 'fence'


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
