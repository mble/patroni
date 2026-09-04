"""Asynchronous, idempotent agent command execution."""
import logging
import math
import re
import time

from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum
from threading import Condition, Event, Lock, RLock, Thread
from typing import Callable, cast, List, NamedTuple, Optional, Tuple, TYPE_CHECKING
from uuid import UUID

from .models import CommandKind, CommandPhase, CommandState, \
    DesiredRole, SlotContext, SlotKind, SlotMember, SlotSpec, SlotTags

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    from .journal import CommandJournal

MAX_COMMAND_HISTORY = 4096
MAX_COMMAND_EVENTS = 128
MAX_TARGET_TEXT = 4096
MAX_SLOT_NAME = 63
SLOT_NAME_RE = re.compile(r'^[a-z0-9_]{1,' + str(MAX_SLOT_NAME) + r'}$')
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
    CommandKind.COPY_SLOTS,
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


class SyncAction(str, Enum):
    """Synchronous configuration operation."""

    REFRESH = 'refresh'
    SET = 'set'


class SyncCount(str, Enum):
    """Whether to pass an explicit synchronous count."""

    DEFAULT = 'default'
    EXPLICIT = 'explicit'


class SyncPlan(NamedTuple):
    """Bounded synchronous standby configuration."""

    action: SyncAction
    members: Tuple[str, ...]
    count_mode: SyncCount
    numsync: Optional[int]


class SlotAction(str, Enum):
    """Replication-slot operation."""

    APPLY = 'apply'
    COPY = 'copy'


class SlotPlan(NamedTuple):
    """Controller-computed replication-slot operation."""

    action: SlotAction
    context: SlotContext
    slots: Tuple[SlotSpec, ...]
    copy_slots: Tuple[str, ...]


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
    REJECTED = 'rejected'


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
    sync_plan: Optional[SyncPlan]
    slot_plan: Optional[SlotPlan]


class DriverResult(NamedTuple):
    """Agent-local driver result and shutdown evidence."""

    value: CommandValue
    checkpoint_location: Optional[int]
    previous_location: Optional[int]
    output: Tuple[str, ...]


class CommandResult(NamedTuple):
    """Observable lifecycle command result."""

    request: LifecycleCommand
    state: CommandState
    value: CommandValue
    checkpoint_location: Optional[int]
    previous_location: Optional[int]
    output: Tuple[str, ...]


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

        with self._changed:
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

            self._changed.notify_all()

            return event

    def events(self, after_sequence: int) -> Tuple[EventRecord, ...]:
        if after_sequence < 0:
            raise ValueError('negative event sequence')

        with self._lock:
            return tuple(event for event in self._events if event.sequence > after_sequence)

    def wait_events(self, after_sequence: int, timeout: Optional[float]) -> Tuple[EventRecord, ...]:
        """Wait bounded time for events after a sequence."""
        if after_sequence < 0:
            raise ValueError('negative event sequence')
        _timeout(timeout)

        deadline = time.monotonic() + timeout if timeout is not None else None
        with self._changed:
            while True:
                events = tuple(event for event in self._events if event.sequence > after_sequence)
                if events or deadline is None:
                    return events

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ()
                self._changed.wait(remaining)

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

    def wake(self) -> None:
        """Wake acknowledgement waits after external cancellation."""
        with self._changed:
            self._changed.notify_all()


class CommandDriver(ABC):
    """Execute lifecycle commands below the agent service."""

    @abstractmethod
    def run(self, command: LifecycleCommand, events: EventChannel,
            cancelled: Event) -> DriverResult:
        """Execute one command."""

    @abstractmethod
    def cancel(self) -> None:
        """Cancel the active driver operation."""

    def fence(self, timeout: Optional[float]) -> bool:
        """Stop PostgreSQL independently from the command worker."""
        self.cancel()
        return False


class _Entry:

    def __init__(self, result: CommandResult) -> None:
        self.result = result
        self.events = EventChannel(result.request.command_id)
        self.cancelled = Event()
        self.done = Event()
        self.thread: Optional[Thread] = None
        self.fence_pending = False


class AgentCommands:
    """Run one agent mutation while observations remain responsive."""

    def __init__(self, driver: CommandDriver, journal: Optional['CommandJournal'] = None) -> None:
        self._driver = driver
        self._journal = journal
        self._lock = RLock()
        self._entries: 'OrderedDict[str, _Entry]' = OrderedDict()
        self._active: Optional[str] = None
        self._closed = Event()
        self._fencing = Event()
        self._fence_done = Event()
        self._fence_done.set()
        self._fence_lock = Lock()
        self._fence_workers = 0
        self._phase_sink: Optional[Callable[[str, CommandPhase], bool]] = None

    def bind_phase(self, sink: Callable[[str, CommandPhase], bool]) -> None:
        """Bind the safety phase observer before command execution."""
        with self._lock:
            if self._phase_sink is not None:
                raise RuntimeError('command phase observer is already bound')
            self._phase_sink = sink

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
            if self._fencing.is_set():
                return CommandSubmission(SubmitState.BUSY, None)
            if self._closed.is_set():
                return CommandSubmission(SubmitState.BUSY, None)

            result = CommandResult(command, CommandState.RUNNING, CommandValue.NONE, None, None, ())
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
            entry.events.wake()
            entry.result = CommandResult(
                entry.result.request, CommandState.CANCELLED, CommandValue.NONE, None, None, (),
            )

        self._driver.cancel()
        return entry.result

    def events(self, command_id: str, after_sequence: int,
               timeout: Optional[float] = None) -> Tuple[EventRecord, ...]:
        _command_id(command_id)
        with self._lock:
            entry = self._entries.get(command_id)
        return entry.events.wait_events(after_sequence, timeout) if entry else ()

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

    def fence(self, timeout: Optional[float]) -> bool:
        """Preempt active work and fence PostgreSQL."""
        _timeout(timeout)
        with self._fence_lock:
            with self._lock:
                entry = self._entries.get(self._active) if self._active else None
                if entry:
                    entry.cancelled.set()
                    entry.events.wake()
                    entry.fence_pending = True
                    self._fence_workers += 1
                    entry.result = CommandResult(
                        entry.result.request, CommandState.FENCED, CommandValue.NONE, None, None, (),
                    )
                self._active = None
                self._fence_done.clear()
                self._fencing.set()

            try:
                return self._driver.fence(timeout)
            finally:
                self._fence_done.set()
                with self._lock:
                    if self._fence_workers == 0:
                        self._fencing.clear()

    def _run(self, entry: _Entry) -> None:
        try:
            if not self._phase(entry, CommandPhase.PREPARING) \
                    or not self._phase(entry, CommandPhase.MUTATING):
                driver_result = DriverResult(CommandValue.NONE, None, None, ())
                state = CommandState.FENCED
            else:
                driver_result = self._driver.run(entry.result.request, entry.events, entry.cancelled)
                _driver_result(driver_result)
                state = CommandState.SUCCEEDED \
                    if driver_result.value != CommandValue.FALSE else CommandState.FAILED
                if not self._phase(entry, CommandPhase.FINALIZING):
                    driver_result = DriverResult(CommandValue.NONE, None, None, ())
                    state = CommandState.FENCED
        except Exception:
            logger.exception('Agent command %s failed', entry.result.request.kind.value)
            driver_result = DriverResult(CommandValue.NONE, None, None, ())
            state = CommandState.FAILED

        with self._lock:
            if entry.result.state == CommandState.FENCED:
                state = CommandState.FENCED
                driver_result = DriverResult(CommandValue.NONE, None, None, ())
            elif entry.cancelled.is_set():
                state = CommandState.CANCELLED
            entry.result = CommandResult(
                entry.result.request,
                state,
                driver_result.value,
                driver_result.checkpoint_location,
                driver_result.previous_location,
                driver_result.output,
            )
            if self._journal:
                try:
                    self._journal.put(entry.result)
                except Exception:
                    entry.result = CommandResult(
                        entry.result.request, CommandState.FAILED, CommandValue.NONE, None, None, (),
                    )
                    self._closed.set()
            entry.done.set()
            if self._active == entry.result.request.command_id:
                self._active = None
            if entry.fence_pending:
                entry.fence_pending = False
                self._fence_workers -= 1
            if self._fencing.is_set() and self._fence_done.is_set() and self._fence_workers == 0:
                self._fencing.clear()
            self._trim()

    def _phase(self, entry: _Entry, phase: CommandPhase) -> bool:
        with self._lock:
            sink = self._phase_sink

        return sink(entry.result.request.command_id, phase) if sink is not None else True

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
    _sync_plan(value.sync_plan)
    _slot_plan(value.slot_plan)
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
    if value.sync_plan is not None and value.kind != CommandKind.APPLY_SYNC:
        raise ValueError('sync plan is not allowed')
    slot_kind = CommandKind.APPLY_SLOTS if value.slot_plan and value.slot_plan.action == SlotAction.APPLY \
        else CommandKind.COPY_SLOTS
    if value.slot_plan is not None and value.kind != slot_kind:
        raise ValueError('slot plan is not allowed')
    if value.kind in (CommandKind.APPLY_SLOTS, CommandKind.COPY_SLOTS) and value.slot_plan is None:
        raise ValueError('slot plan is required')
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


def _driver_result(value: object) -> None:
    if not isinstance(value, DriverResult) or not isinstance(cast(object, value.value), CommandValue):
        raise ValueError('invalid driver result')
    _location(value.checkpoint_location)
    _location(value.previous_location)
    if len(value.output) > MAX_COMMAND_HISTORY:
        raise ValueError('driver output exceeds limit')
    for item in value.output:
        raw_item = cast(object, item)
        _slot_name(raw_item)


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


def _sync_plan(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, SyncPlan) or not isinstance(cast(object, value.action), SyncAction) \
            or not isinstance(cast(object, value.count_mode), SyncCount):
        raise ValueError('invalid sync plan')
    if len(value.members) > MAX_COMMAND_HISTORY or len(set(name.lower() for name in value.members)) \
            != len(value.members):
        raise ValueError('invalid sync members')
    for name in value.members:
        raw_name = cast(object, name)
        if not isinstance(raw_name, str) or not raw_name \
                or len(raw_name) > MAX_TARGET_TEXT or '\x00' in raw_name:
            raise ValueError('invalid sync member')
    raw_numsync = cast(object, value.numsync)
    if raw_numsync is not None and (not isinstance(raw_numsync, int)
                                    or isinstance(raw_numsync, bool) or raw_numsync < 0):
        raise ValueError('invalid synchronous node count')
    if value.action == SyncAction.REFRESH and (value.members or value.count_mode != SyncCount.DEFAULT
                                               or value.numsync is not None):
        raise ValueError('invalid sync refresh plan')
    if value.count_mode == SyncCount.DEFAULT and value.numsync is not None:
        raise ValueError('unexpected synchronous node count')


def _slot_plan(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, SlotPlan) or not isinstance(cast(object, value.action), SlotAction) \
            or not isinstance(cast(object, value.context), SlotContext):
        raise ValueError('invalid slot plan')
    if len(value.slots) > MAX_COMMAND_HISTORY or len(value.copy_slots) > MAX_COMMAND_HISTORY \
            or len(value.context.members) > MAX_COMMAND_HISTORY:
        raise ValueError('slot plan exceeds limit')
    _text(value.context.local_name, 'local member')
    if not isinstance(cast(object, value.context.config_present), bool):
        raise ValueError('invalid slot context')
    if value.context.leader is not None:
        _text(value.context.leader, 'slot leader')
    if value.action == SlotAction.APPLY and value.copy_slots:
        raise ValueError('apply plan contains copy slots')
    _slot_tags(value.context.local_tags)
    names: set[str] = set()
    for member in value.context.members:
        if not isinstance(cast(object, member), SlotMember):
            raise ValueError('invalid slot member')
        _text(member.name, 'slot member')
        for text in (member.host, member.port, member.database):
            if text is not None:
                _text(text, 'slot endpoint')
        _slot_tags(member.tags)
        _location(member.lsn)
    for spec in value.slots:
        if not isinstance(cast(object, spec), SlotSpec) or not isinstance(cast(object, spec.kind), SlotKind):
            raise ValueError('invalid slot specification')
        _slot_name(spec.name)
        if spec.name in names:
            raise ValueError('duplicate slot name')
        names.add(spec.name)
        for text in (spec.database, spec.plugin):
            if text is not None:
                _text(text, 'slot attribute')
        _location(spec.lsn)
        for flag in (spec.expected_active, spec.failover):
            if flag is not None and not isinstance(cast(object, flag), bool):
                raise ValueError('invalid slot flag')
    for name in value.copy_slots:
        _slot_name(name)
        if name not in names:
            raise ValueError('unknown copy slot')
    for name, location in value.context.status_slots:
        _slot_name(name)
        _location(location)
    for name in value.context.retain_slots:
        _slot_name(name)


def _slot_tags(value: object) -> None:
    if not isinstance(value, SlotTags):
        raise ValueError('invalid slot tags')
    if not isinstance(cast(object, value.nofailover), bool) \
            or not isinstance(cast(object, value.nostream), bool):
        raise ValueError('invalid slot tags')
    if value.replicatefrom is not None:
        _text(value.replicatefrom, 'replication source')


def _text(value: object, field: str) -> None:
    if not isinstance(value, str) or not value or len(value) > MAX_TARGET_TEXT or '\x00' in value:
        raise ValueError('invalid {0}'.format(field))


def _slot_name(value: object) -> None:
    if not isinstance(value, str) or not SLOT_NAME_RE.fullmatch(value):
        raise ValueError('invalid slot name')
