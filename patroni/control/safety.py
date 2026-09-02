"""Pure primary-authority state machine."""
import math

from collections import OrderedDict
from enum import Enum
from typing import Callable, Optional, Tuple, Type
from uuid import UUID

from .models import AgentState, AuthorityGrant, AuthorityKind, AuthorityState, CommandKind, \
    CommandPhase, CommandReceipt, CommandRequest, CommandState, CommandStatus, DesiredRole, \
    PolicyMode, PostgresRole, SafetyAction, SafetySnapshot, Timing

MAX_COMMAND_HISTORY = 4096
MAX_COUNTER = (1 << 63) - 1
TERMINAL_COMMAND_STATES = frozenset((
    CommandState.SUCCEEDED,
    CommandState.FAILED,
    CommandState.CANCELLED,
    CommandState.FENCED,
))
SAFE_WITHOUT_AUTHORITY = frozenset((
    CommandKind.STOP,
    CommandKind.FOLLOW,
    CommandKind.FENCE,
))
FAILSAFE_PRIMARY_COMMANDS = frozenset((
    CommandKind.APPLY_CONFIG,
    CommandKind.APPLY_SYNC,
    CommandKind.APPLY_SLOTS,
    CommandKind.CHECKPOINT,
))
PHASE_ORDER = {
    CommandPhase.ACCEPTED: 0,
    CommandPhase.PREPARING: 1,
    CommandPhase.MUTATING: 2,
    CommandPhase.FINALIZING: 3,
}


class ValidationError(ValueError):
    """Input failed closed without changing safety state."""


class SafetyState:
    """Validate authority and serialize PostgreSQL mutations."""

    def __init__(self, agent_boot_id: str, controller_boot_id: str,
                 clock: Callable[[], float], history_limit: int) -> None:
        _boot_id(agent_boot_id, 'agent_boot_id')
        _boot_id(controller_boot_id, 'controller_boot_id')
        if history_limit < 1 or history_limit > MAX_COMMAND_HISTORY:
            raise ValidationError('history_limit is out of range')

        self._agent_boot_id = agent_boot_id
        self._controller_boot_id = controller_boot_id
        self._clock = clock
        self._history_limit = history_limit
        self._agent_state = AgentState.IDLE
        self._policy_mode = PolicyMode.ACTIVE
        self._postgres_role = PostgresRole.UNKNOWN
        self._last_sequence = 0
        self._authority: Optional[AuthorityGrant] = None
        self._active_command_id: Optional[str] = None
        self._commands: 'OrderedDict[str, CommandStatus]' = OrderedDict()
        self._connected = True

    @property
    def snapshot(self) -> SafetySnapshot:
        """Return immutable state without exposing mutable records."""
        authority = self._authority
        authority_state = AuthorityState.ABSENT
        if authority:
            authority_state = AuthorityState.CURRENT if self._clock() < authority.deadline else AuthorityState.EXPIRED

        return SafetySnapshot(
            self._agent_state,
            self._policy_mode,
            self._postgres_role,
            self._controller_boot_id,
            self._agent_boot_id,
            self._last_sequence,
            authority_state,
            authority.kind if authority else None,
            authority.term if authority else 0,
            authority.deadline if authority else 0.0,
            self._active_command_id,
            len(self._commands),
            self._connected,
        )

    def grant(self, grant: AuthorityGrant) -> None:
        """Install a validated authority grant."""
        self._validate_grant(grant)

        if grant.sequence == self._last_sequence:
            return

        self._authority = grant
        self._last_sequence = grant.sequence
        self._connected = True

    def policy(self, mode: PolicyMode, sequence: int) -> SafetyAction:
        """Apply active or paused Patroni policy."""
        _enum(mode, PolicyMode, 'policy mode')
        self._sequence(sequence)

        self._policy_mode = mode
        self._last_sequence = sequence

        return self._guard()

    def submit(self, request: CommandRequest) -> CommandReceipt:
        """Validate and record one idempotent command."""
        fingerprint = self._validate_request(request)
        previous = self._commands.get(request.command_id)
        if previous:
            if fingerprint != self._fingerprint(previous.request):
                raise ValidationError('command ID reused with different input')
            if request.sequence > self._last_sequence:
                self._last_sequence = request.sequence

            return CommandReceipt(SafetyAction.NONE, previous.state)

        self._sequence(request.sequence)
        self._last_sequence = request.sequence

        if request.kind == CommandKind.FENCE:
            status = CommandStatus(request, CommandPhase.ACCEPTED, CommandState.FENCED)
            self._commands[request.command_id] = status
            action = self._start_fence()
            self._trim_history()
            return CommandReceipt(action, status.state)

        if self._active_command_id:
            return CommandReceipt(SafetyAction.REJECT, CommandState.FAILED)
        if self._agent_state == AgentState.FENCING:
            return CommandReceipt(SafetyAction.REJECT, CommandState.FAILED)
        if not self._allows(request):
            return CommandReceipt(SafetyAction.REJECT, CommandState.FAILED)

        status = CommandStatus(request, CommandPhase.ACCEPTED, CommandState.RUNNING)
        self._commands[request.command_id] = status
        self._active_command_id = request.command_id
        self._agent_state = AgentState.BUSY
        self._trim_history()

        return CommandReceipt(SafetyAction.RUN, status.state)

    def advance(self, command_id: str, phase: CommandPhase) -> SafetyAction:
        """Advance an active command after rechecking authority."""
        status = self._active(command_id)
        _enum(phase, CommandPhase, 'command phase')
        if PHASE_ORDER[phase] < PHASE_ORDER[status.phase]:
            raise ValidationError('command phase moved backwards')

        action = self._guard()
        if action == SafetyAction.FENCE:
            return action

        self._commands[command_id] = status._replace(phase=phase)
        return SafetyAction.RUN

    def complete(self, command_id: str, state: CommandState) -> SafetyAction:
        """Record one terminal command result."""
        status = self._active(command_id)
        if state not in TERMINAL_COMMAND_STATES:
            raise ValidationError('invalid terminal command state')

        action = self._guard()
        if action == SafetyAction.FENCE:
            return action

        self._commands[command_id] = status._replace(state=state)
        self._active_command_id = None
        self._agent_state = AgentState.IDLE
        self._trim_history()
        return SafetyAction.NONE

    def command(self, command_id: str) -> CommandStatus:
        """Return retained command state."""
        try:
            return self._commands[command_id]
        except KeyError:
            raise KeyError(command_id)

    def observe(self, role: PostgresRole) -> SafetyAction:
        """Record PostgreSQL role and enforce primary authority."""
        _enum(role, PostgresRole, 'PostgreSQL role')

        self._postgres_role = role
        return self._guard()

    def tick(self) -> SafetyAction:
        """Check the current deadline without polling."""
        return self._guard()

    def disconnect(self) -> None:
        """Record controller loss while retaining the current deadline."""
        self._connected = False

    def fence(self) -> SafetyAction:
        """Preempt work and request unconditional fencing."""
        return self._start_fence()

    def fence_complete(self, role: PostgresRole) -> None:
        """Acknowledge a completed fence after observing a safe role."""
        _enum(role, PostgresRole, 'PostgreSQL role')
        if self._agent_state != AgentState.FENCING:
            raise ValidationError('fence is not active')
        if role == PostgresRole.PRIMARY:
            raise ValidationError('fence did not remove primary role')

        self._postgres_role = role
        self._agent_state = AgentState.IDLE

    def _active(self, command_id: str) -> CommandStatus:
        if command_id != self._active_command_id:
            raise ValidationError('command is not active')

        return self._commands[command_id]

    def _allows(self, request: CommandRequest) -> bool:
        if request.kind in SAFE_WITHOUT_AUTHORITY:
            return True

        authority = self._current_authority()
        if request.kind == CommandKind.BOOTSTRAP:
            return bool(authority and authority.term == request.authority_term
                        and authority.kind == AuthorityKind.INITIALIZER)
        if self._postgres_role == PostgresRole.PRIMARY:
            if not authority or authority.term != request.authority_term:
                return False
            if authority.kind == AuthorityKind.LEADER:
                return True

            return authority.kind == AuthorityKind.FAILSAFE and request.kind in FAILSAFE_PRIMARY_COMMANDS

        if request.target_role != DesiredRole.PRIMARY:
            return True
        if not authority or authority.term != request.authority_term:
            return False

        return authority.kind == AuthorityKind.LEADER

    def _command_needs_authority(self) -> bool:
        if not self._active_command_id:
            return False

        request = self._commands[self._active_command_id].request
        if request.kind in SAFE_WITHOUT_AUTHORITY:
            return False

        return self._postgres_role == PostgresRole.PRIMARY or request.target_role == DesiredRole.PRIMARY

    def _current_authority(self) -> Optional[AuthorityGrant]:
        authority = self._authority
        if authority and self._clock() < authority.deadline:
            return authority

        return None

    def _guard(self) -> SafetyAction:
        if self._policy_mode == PolicyMode.PAUSED:
            return SafetyAction.NONE
        if self._postgres_role == PostgresRole.PRIMARY and not self._current_authority():
            return self._start_fence()
        if self._command_needs_authority() and self._active_command_id:
            request = self._commands[self._active_command_id].request
            if not self._allows(request):
                return self._start_fence()

        return SafetyAction.NONE

    def _start_fence(self) -> SafetyAction:
        if self._active_command_id:
            status = self._commands[self._active_command_id]
            self._commands[self._active_command_id] = status._replace(state=CommandState.FENCED)
            self._active_command_id = None

        self._agent_state = AgentState.FENCING
        self._trim_history()
        return SafetyAction.FENCE

    def _trim_history(self) -> None:
        while len(self._commands) > self._history_limit:
            command_id = next(iter(self._commands))
            if command_id == self._active_command_id:
                raise ValidationError('active command exceeds history bound')
            self._commands.popitem(last=False)

    def _validate_grant(self, grant: AuthorityGrant) -> None:
        _enum(grant.kind, AuthorityKind, 'authority kind')
        self._identities(grant.controller_boot_id, grant.agent_boot_id)
        _counter(grant.term, 'term')
        _counter(grant.sequence, 'sequence')
        _timing(grant.timing)
        _finite(grant.issued_at, 'issued_at')
        _finite(grant.deadline, 'deadline')

        now = self._clock()
        if grant.issued_at > now:
            raise ValidationError('grant issued in the future')
        if grant.deadline <= now:
            raise ValidationError('grant already expired')

        safe_lifetime = grant.timing.ttl
        if grant.timing.watchdog_timeout is not None:
            safe_lifetime = min(safe_lifetime, grant.timing.watchdog_timeout)
        if grant.deadline > grant.issued_at + safe_lifetime:
            raise ValidationError('grant exceeds Patroni safety deadline')

        authority = self._authority
        if authority and grant.term < authority.term:
            raise ValidationError('authority term moved backwards')
        if authority and grant.term == authority.term and grant.kind != authority.kind:
            raise ValidationError('authority kind changed within a term')
        if grant.sequence == self._last_sequence and grant == authority:
            return

        self._sequence(grant.sequence)

    def _validate_request(self, request: CommandRequest) -> Tuple[CommandKind, DesiredRole, int]:
        _boot_id(request.command_id, 'command_id')
        self._identities(request.controller_boot_id, request.agent_boot_id)
        _counter(request.sequence, 'sequence')
        _authority_term(request.authority_term)
        _enum(request.kind, CommandKind, 'command kind')
        _enum(request.target_role, DesiredRole, 'desired role')

        return self._fingerprint(request)

    @staticmethod
    def _fingerprint(request: CommandRequest) -> Tuple[CommandKind, DesiredRole, int]:
        return request.kind, request.target_role, request.authority_term

    def _identities(self, controller_boot_id: str, agent_boot_id: str) -> None:
        _boot_id(controller_boot_id, 'controller_boot_id')
        _boot_id(agent_boot_id, 'agent_boot_id')
        if controller_boot_id != self._controller_boot_id:
            raise ValidationError('controller boot ID mismatch')
        if agent_boot_id != self._agent_boot_id:
            raise ValidationError('agent boot ID mismatch')

    def _sequence(self, sequence: int) -> None:
        _counter(sequence, 'sequence')
        if sequence <= self._last_sequence:
            raise ValidationError('sequence did not increase')


def _boot_id(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise ValidationError('{0} is not text'.format(name))

    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise ValidationError('{0} is not a UUID'.format(name))

    if value != str(parsed):
        raise ValidationError('{0} is not canonical'.format(name))


def _counter(value: object, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError('{0} is not an integer'.format(name))
    if value < 1 or value > MAX_COUNTER:
        raise ValidationError('{0} is out of range'.format(name))


def _authority_term(value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError('authority_term is not an integer')
    if value < 0 or value > MAX_COUNTER:
        raise ValidationError('authority_term is out of range')


def _finite(value: object, name: str) -> None:
    if not isinstance(value, (float, int)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValidationError('{0} is not finite'.format(name))


def _timing(timing: object) -> None:
    if not isinstance(timing, Timing):
        raise ValidationError('invalid timing')

    values = (timing.ttl, timing.loop_wait, timing.retry_timeout)
    for value in values:
        _finite(value, 'timing')
        if value <= 0:
            raise ValidationError('timing must be positive')

    if timing.loop_wait + 2 * timing.retry_timeout > timing.ttl:
        raise ValidationError('loop_wait + 2 * retry_timeout exceeds ttl')
    if timing.watchdog_timeout is None:
        return

    _finite(timing.watchdog_timeout, 'watchdog_timeout')
    if timing.watchdog_timeout <= 0 or timing.watchdog_timeout > timing.ttl:
        raise ValidationError('watchdog timeout is out of range')


def _enum(value: object, enum_type: Type[Enum], name: str) -> None:
    if not isinstance(value, enum_type):
        raise ValidationError('invalid {0}'.format(name))
