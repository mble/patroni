"""Asynchronous, idempotent agent command execution."""
import math
import time

from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum
from threading import Condition, Event, RLock, Thread
from typing import cast, List, NamedTuple, Optional, Tuple, TYPE_CHECKING
from uuid import UUID

from .models import CommandKind, CommandState, DesiredRole

if TYPE_CHECKING:  # pragma: no cover
    from .journal import CommandJournal

MAX_COMMAND_HISTORY = 4096
MAX_COMMAND_EVENTS = 128
MAX_TARGET_TEXT = 4096
AGENT_KINDS = frozenset((
    CommandKind.START,
    CommandKind.STOP,
    CommandKind.RESTART,
    CommandKind.PROMOTE,
    CommandKind.FOLLOW,
    CommandKind.FENCE,
    CommandKind.BOOTSTRAP,
    CommandKind.CLONE,
    CommandKind.REWIND,
    CommandKind.CRASH_RECOVERY,
    CommandKind.POST_BOOTSTRAP,
    CommandKind.REINITIALIZE,
    CommandKind.APPLY_CONFIG,
    CommandKind.APPLY_SYNC,
    CommandKind.APPLY_SLOTS,
    CommandKind.CALLBACK,
    CommandKind.REMOVE_DATA,
    CommandKind.MOVE_DATA,
    CommandKind.SET_BOOTSTRAP,
    CommandKind.RESET_RECOVERY,
    CommandKind.CHECK_DIVERGENCE,
    CommandKind.CHECKPOINT,
    CommandKind.ARCHIVE_WAL,
))


class StopMode(str, Enum):
    """PostgreSQL shutdown mode."""

    DEFAULT = 'default'
    SMART = 'smart'
    FAST = 'fast'
    IMMEDIATE = 'immediate'


class CheckpointMode(str, Enum):
    """Pre-shutdown checkpoint policy."""

    DEFAULT = 'default'
    ENABLED = 'enabled'
    DISABLED = 'disabled'


class ReloadMode(str, Enum):
    """Follow configuration activation mode."""

    RESTART = 'restart'
    RELOAD = 'reload'


class CloneMode(str, Enum):
    """Replica source selection policy."""

    CONFIGURED = 'configured'
    LEADER = 'leader'


class BootstrapState(str, Enum):
    """Bootstrap activity state."""

    IDLE = 'idle'
    RUNNING = 'running'


class CancelMode(str, Enum):
    """Agent subprocess cancellation mode."""

    NORMAL = 'normal'
    KILL = 'kill'


class CallbackKind(str, Enum):
    """Allowlisted PostgreSQL callback."""

    START = 'on_start'
    ROLE_CHANGE = 'on_role_change'


class DivergencePolicy(str, Enum):
    """Controller-selected divergence action."""

    NONE = 'none'
    REWIND = 'rewind'
    REINITIALIZE = 'reinitialize'


class TargetKind(str, Enum):
    """Follow target source."""

    MEMBER = 'member'
    REMOTE = 'remote'


class SlotMode(str, Enum):
    """Remote-member replication slot policy."""

    USE = 'use'
    DISABLE = 'disable'


class CommandValue(str, Enum):
    """Stable representation of PostgreSQL's tri-state result."""

    TRUE = 'true'
    FALSE = 'false'
    PENDING = 'pending'
    NONE = 'none'


class SubmitState(str, Enum):
    """Command submission outcome."""

    ACCEPTED = 'accepted'
    REPLAYED = 'replayed'
    BUSY = 'busy'
    CONFLICT = 'conflict'


class EventKind(str, Enum):
    """Lifecycle progress requiring controller action."""

    SAFEPOINT = 'safepoint'
    AFTER_START = 'after_start'
    BEFORE_PROMOTE = 'before_promote'
    BEFORE_SHUTDOWN = 'before_shutdown'
    SHUTDOWN = 'shutdown'


class AckState(str, Enum):
    """Controller acknowledgement outcome."""

    ACKED = 'acked'
    CANCELLED = 'cancelled'
    TIMED_OUT = 'timed_out'


class FollowTarget(NamedTuple):
    """Credential-free PostgreSQL follow endpoint."""

    kind: TargetKind
    name: str
    host: Optional[str]
    port: Optional[str]
    database: Optional[str]
    slot_name: Optional[str]
    slot_mode: SlotMode


class RecoveryTarget(NamedTuple):
    """Credential-free rewind source."""

    kind: TargetKind
    name: str
    host: Optional[str]
    port: Optional[str]
    database: Optional[str]
    slot_name: Optional[str]
    slot_mode: SlotMode
    role: Optional[str]
    checkpoint_after_promote: Optional[bool]


class LifecycleCommand(NamedTuple):
    """Bounded lifecycle command data."""

    command_id: str
    kind: CommandKind
    target_role: DesiredRole
    timeout: Optional[float]
    stop_mode: StopMode
    checkpoint: CheckpointMode
    events: Tuple[EventKind, ...]
    follow_target: Optional[FollowTarget]
    reload: ReloadMode
    recovery_target: Optional[RecoveryTarget]
    clone_mode: CloneMode
    divergence: DivergencePolicy
    callback: Optional[CallbackKind]
    bootstrap_state: BootstrapState


class DriverResult(NamedTuple):
    """Agent-local driver result and shutdown evidence."""

    value: CommandValue
    checkpoint_location: Optional[int]
    previous_location: Optional[int]


class CommandResult(NamedTuple):
    """Observable lifecycle command result."""

    request: LifecycleCommand
    state: CommandState
    value: CommandValue
    checkpoint_location: Optional[int]
    previous_location: Optional[int]


class CommandSubmission(NamedTuple):
    """Immediate command submission result."""

    state: SubmitState
    result: Optional[CommandResult]


class EventRecord(NamedTuple):
    """Sequenced shutdown evidence."""

    command_id: str
    sequence: int
    kind: EventKind
    checkpoint_location: Optional[int]
    previous_location: Optional[int]


class EventChannel:
    """Retain bounded, acknowledged command events."""

    def __init__(self, command_id: str) -> None:
        self._command_id = command_id
        self._lock = RLock()
        self._changed = Condition(self._lock)
        self._sequence = 0
        self._acked = 0
        self._events: List[EventRecord] = []

    def publish(self, kind: object, checkpoint_location: Optional[int] = None,
                previous_location: Optional[int] = None) -> EventRecord:
        if not isinstance(kind, EventKind):
            raise ValueError('invalid event kind')
        event_kind = kind
        _location(checkpoint_location)
        _location(previous_location)
        if event_kind == EventKind.SHUTDOWN and (checkpoint_location is None or previous_location is None):
            raise ValueError('shutdown event requires WAL locations')

        with self._lock:
            self._sequence += 1
            event = EventRecord(
                self._command_id,
                self._sequence,
                event_kind,
                checkpoint_location,
                previous_location,
            )
            self._events.append(event)
            if len(self._events) > MAX_COMMAND_EVENTS:
                self._events.pop(0)

            return event

    def events(self, after_sequence: int) -> Tuple[EventRecord, ...]:
        if after_sequence < 0:
            raise ValueError('negative event sequence')

        with self._lock:
            return tuple(event for event in self._events if event.sequence > after_sequence)

    def ack(self, sequence: int) -> None:
        if sequence < 0:
            raise ValueError('negative event sequence')

        with self._changed:
            if sequence > self._sequence:
                raise ValueError('unknown event sequence')

            self._acked = max(self._acked, sequence)
            self._events = [event for event in self._events if event.sequence > sequence]
            self._changed.notify_all()

    def wait_ack(self, sequence: int, timeout: float, cancelled: Event) -> AckState:
        _timeout(timeout)
        if sequence <= 0:
            raise ValueError('invalid event sequence')

        deadline = time.monotonic() + timeout
        with self._changed:
            while self._acked < sequence:
                if cancelled.is_set():
                    return AckState.CANCELLED

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return AckState.TIMED_OUT
                self._changed.wait(remaining)

            return AckState.ACKED


class CommandDriver(ABC):
    """Execute lifecycle commands below the agent service."""

    @abstractmethod
    def run(self, command: LifecycleCommand, events: EventChannel,
            cancelled: Event) -> DriverResult:
        """Execute one command."""

    @abstractmethod
    def cancel(self) -> None:
        """Cancel the active driver operation."""


class _Entry:

    def __init__(self, result: CommandResult) -> None:
        self.result = result
        self.events = EventChannel(result.request.command_id)
        self.cancelled = Event()
        self.done = Event()
        self.thread: Optional[Thread] = None


class AgentCommands:
    """Run one agent mutation while observations remain responsive."""

    def __init__(self, driver: CommandDriver, journal: Optional['CommandJournal'] = None) -> None:
        self._driver = driver
        self._journal = journal
        self._lock = RLock()
        self._entries: 'OrderedDict[str, _Entry]' = OrderedDict()
        self._active: Optional[str] = None
        self._closed = Event()

    def submit(self, command: LifecycleCommand) -> CommandSubmission:
        _command(command)

        with self._lock:
            entry = self._entries.get(command.command_id)
            if entry:
                state = SubmitState.REPLAYED if entry.result.request == command else SubmitState.CONFLICT
                return CommandSubmission(state, entry.result)
            if self._journal:
                from .journal import JournalError
                try:
                    result = self._journal.get(command)
                except JournalError:
                    return CommandSubmission(SubmitState.CONFLICT, None)
                if result:
                    entry = _Entry(result)
                    entry.done.set()
                    self._entries[command.command_id] = entry
                    return CommandSubmission(SubmitState.REPLAYED, result)
            if self._active is not None:
                return CommandSubmission(SubmitState.BUSY, None)
            if self._closed.is_set():
                return CommandSubmission(SubmitState.BUSY, None)

            result = CommandResult(command, CommandState.RUNNING, CommandValue.NONE, None, None)
            entry = _Entry(result)
            self._entries[command.command_id] = entry
            self._active = command.command_id
            thread = Thread(target=self._run, args=(entry,), name='agent-command')
            thread.daemon = True
            entry.thread = thread
            thread.start()

            return CommandSubmission(SubmitState.ACCEPTED, result)

    def status(self, command_id: str) -> Optional[CommandResult]:
        _command_id(command_id)
        with self._lock:
            entry = self._entries.get(command_id)
            return entry.result if entry else None

    def active(self) -> Optional[CommandResult]:
        with self._lock:
            entry = self._entries.get(self._active) if self._active else None
            return entry.result if entry else None

    def wait(self, command_id: str, timeout: Optional[float]) -> Optional[CommandResult]:
        _timeout(timeout)
        with self._lock:
            entry = self._entries.get(command_id)
        if not entry:
            return None

        entry.done.wait(timeout)
        return self.status(command_id)

    def cancel(self, command_id: str) -> Optional[CommandResult]:
        _command_id(command_id)
        with self._lock:
            entry = self._entries.get(command_id)
            if not entry:
                return None
            if entry.result.state != CommandState.RUNNING:
                return entry.result

            entry.cancelled.set()
            entry.result = CommandResult(entry.result.request, CommandState.CANCELLED, CommandValue.NONE, None, None)

        self._driver.cancel()
        return entry.result

    def events(self, command_id: str, after_sequence: int) -> Tuple[EventRecord, ...]:
        _command_id(command_id)
        with self._lock:
            entry = self._entries.get(command_id)
        return entry.events.events(after_sequence) if entry else ()

    def ack(self, command_id: str, sequence: int) -> None:
        _command_id(command_id)
        with self._lock:
            entry = self._entries.get(command_id)
        if entry:
            entry.events.ack(sequence)

    def close(self) -> None:
        self._closed.set()
        with self._lock:
            entry = self._entries.get(self._active) if self._active else None
        if entry:
            self.cancel(entry.result.request.command_id)
            if entry.thread:
                entry.thread.join(1)

    def _run(self, entry: _Entry) -> None:
        try:
            driver_result = self._driver.run(entry.result.request, entry.events, entry.cancelled)
            state = CommandState.SUCCEEDED if driver_result.value != CommandValue.FALSE else CommandState.FAILED
        except Exception:
            driver_result = DriverResult(CommandValue.NONE, None, None)
            state = CommandState.FAILED

        with self._lock:
            if entry.cancelled.is_set():
                state = CommandState.CANCELLED
            entry.result = CommandResult(
                entry.result.request,
                state,
                driver_result.value,
                driver_result.checkpoint_location,
                driver_result.previous_location,
            )
            if self._journal:
                try:
                    self._journal.put(entry.result)
                except Exception:
                    entry.result = CommandResult(
                        entry.result.request, CommandState.FAILED, CommandValue.NONE, None, None,
                    )
                    self._closed.set()
            entry.done.set()
            self._active = None
            self._trim()

    def _trim(self) -> None:
        while len(self._entries) > MAX_COMMAND_HISTORY:
            command_id, entry = next(iter(self._entries.items()))
            if entry.result.state == CommandState.RUNNING:
                return
            del self._entries[command_id]


def _command(value: object) -> None:
    if not isinstance(value, LifecycleCommand):
        raise ValueError('invalid lifecycle command')
    _command_id(value.command_id)
    if not isinstance(cast(object, value.kind), CommandKind) or value.kind not in AGENT_KINDS:
        raise ValueError('invalid lifecycle command kind')
    if not isinstance(cast(object, value.target_role), DesiredRole):
        raise ValueError('invalid target role')
    if not isinstance(cast(object, value.stop_mode), StopMode):
        raise ValueError('invalid stop mode')
    if not isinstance(cast(object, value.checkpoint), CheckpointMode):
        raise ValueError('invalid checkpoint mode')
    if len(value.events) > len(EventKind) or len(set(value.events)) != len(value.events) \
            or any(not isinstance(cast(object, kind), EventKind) for kind in value.events):
        raise ValueError('invalid lifecycle events')
    _target(value.follow_target)
    if not isinstance(cast(object, value.reload), ReloadMode):
        raise ValueError('invalid reload mode')
    _recovery_target(value.recovery_target)
    if not isinstance(cast(object, value.clone_mode), CloneMode):
        raise ValueError('invalid clone mode')
    if not isinstance(cast(object, value.divergence), DivergencePolicy):
        raise ValueError('invalid divergence policy')
    if value.callback is not None and not isinstance(cast(object, value.callback), CallbackKind):
        raise ValueError('invalid callback')
    if not isinstance(cast(object, value.bootstrap_state), BootstrapState):
        raise ValueError('invalid bootstrap state')
    if value.follow_target is not None and value.kind != CommandKind.FOLLOW:
        raise ValueError('follow target is not allowed')
    recovery_kinds = (CommandKind.CLONE, CommandKind.REWIND, CommandKind.REINITIALIZE)
    if value.recovery_target is not None and value.kind not in recovery_kinds:
        raise ValueError('recovery target is not allowed')
    if value.kind in (CommandKind.REWIND, CommandKind.REINITIALIZE) and value.recovery_target is None:
        raise ValueError('recovery target is required')
    if value.clone_mode != CloneMode.CONFIGURED and value.kind not in (CommandKind.CLONE, CommandKind.REINITIALIZE):
        raise ValueError('clone mode is not allowed')
    expected_policy = {
        CommandKind.REWIND: DivergencePolicy.REWIND,
        CommandKind.REINITIALIZE: DivergencePolicy.REINITIALIZE,
    }.get(value.kind, DivergencePolicy.NONE)
    if value.divergence != expected_policy:
        raise ValueError('invalid divergence policy')
    if value.callback is not None and value.kind != CommandKind.CALLBACK:
        raise ValueError('callback is not allowed')
    if value.kind == CommandKind.CALLBACK and value.callback is None:
        raise ValueError('callback is required')
    _timeout(value.timeout)


def _command_id(value: str) -> None:
    try:
        valid = value == str(UUID(value))
    except (AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError('invalid command ID')


def _timeout(value: object) -> None:
    if value is not None and (not isinstance(value, (float, int))
                              or not math.isfinite(cast(float, value)) or value < 0):
        raise ValueError('invalid command timeout')


def _location(value: object) -> None:
    if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
        raise ValueError('invalid WAL location')


def _target(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, FollowTarget):
        raise ValueError('invalid follow target')
    if not isinstance(cast(object, value.kind), TargetKind) \
            or not isinstance(cast(object, value.slot_mode), SlotMode):
        raise ValueError('invalid follow target mode')
    for text in (value.name,):
        raw_text = cast(object, text)
        if not isinstance(raw_text, str) or len(raw_text) > MAX_TARGET_TEXT or '\x00' in raw_text:
            raise ValueError('invalid follow target field')
    for text in (value.host, value.port, value.database):
        raw_text = cast(object, text)
        if raw_text is not None and (not isinstance(raw_text, str)
                                     or len(raw_text) > MAX_TARGET_TEXT or '\x00' in raw_text):
            raise ValueError('invalid follow endpoint')
    raw_slot = cast(object, value.slot_name)
    if raw_slot is not None and (not isinstance(raw_slot, str)
                                 or len(raw_slot) > MAX_TARGET_TEXT or '\x00' in raw_slot):
        raise ValueError('invalid follow slot')


def _recovery_target(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, RecoveryTarget):
        raise ValueError('invalid recovery target')
    if not isinstance(cast(object, value.kind), TargetKind) \
            or not isinstance(cast(object, value.slot_mode), SlotMode):
        raise ValueError('invalid recovery target mode')
    for text in (value.name, value.host, value.port, value.database, value.slot_name, value.role):
        raw_text = cast(object, text)
        if raw_text is not None and (not isinstance(raw_text, str)
                                     or len(raw_text) > MAX_TARGET_TEXT or '\x00' in raw_text):
            raise ValueError('invalid recovery target field')
    checkpoint = cast(object, value.checkpoint_after_promote)
    if checkpoint is not None and not isinstance(checkpoint, bool):
        raise ValueError('invalid recovery checkpoint state')
