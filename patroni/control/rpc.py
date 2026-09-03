"""NodeControl RPC service and controller client."""
import hashlib
import logging
import socket
import time

from collections import OrderedDict
from threading import Lock, RLock
from typing import Any, Callable, cast, Dict, Optional, Sequence, Tuple
from uuid import UUID, uuid4

from .commands import CancelMode, CommandResult, CommandSubmission, \
    EventRecord, LifecycleCommand, RecoveryTarget, SubmitState
from .models import AgentState, AgentTelemetry, AuthorityGrant, AuthorityKind, CommandRequest, \
    CommandState, ConfigApply, ConfigChange, DynamicConfigPlan, FenceReason, Freshness, \
    NodeSnapshot, ObservationContext, ObservationFailure, PolicyMode, PostgresRole, PostgresState, \
    RecoverySnapshot, SafetyAction, SlotCapabilities, SnapshotDetail, SyncContext, SyncSnapshot, \
    TimelineWal, Timing, WalObservation, WatchdogReload, WatchdogSnapshot, WatchdogTiming
from .node import NodeControl
from .protocol import Capability, ErrorCode, Hello, NodeCall, Operation, pack, PROTOCOL_MAJOR, \
    PROTOCOL_MINOR, ProtocolError, read_frame, Request, Response, write_frame
from .safety import SafetyState, ValidationError
from .unix import peer_check as unix_peer_check

logger = logging.getLogger(__name__)

MAX_REQUEST_HISTORY = 4096
DEFAULT_RPC_TIMEOUT = 5.0
MAX_RPC_ATTEMPTS = 2
SAFETY_HISTORY = 4096
CAPABILITIES = tuple(Capability)
ZERO_NODE_CALLS = frozenset((
    NodeCall.IS_PRIMARY,
    NodeCall.IS_RUNNING,
    NodeCall.IS_STARTING,
    NodeCall.LAST_OPERATION,
    NodeCall.TIMELINE_WAL,
    NodeCall.REPLICATION_STATE,
    NodeCall.RECEIVED_TIMELINE,
    NodeCall.CONTROL_TIMELINE,
    NodeCall.POSTMASTER_START,
    NodeCall.SERVER_VERSION,
    NodeCall.SLOTS,
    NodeCall.CHECKPOINT_LOCATIONS,
    NodeCall.RECOVERY,
    NodeCall.CAN_REWIND,
    NodeCall.ARCHIVE_READY,
    NodeCall.DATA_EMPTY,
    NodeCall.CONTROLDATA,
    NodeCall.RESTORED,
    NodeCall.RECOVERY_CONF_EXISTS,
    NodeCall.RESET_CANCEL,
    NodeCall.WATCHDOG,
    NodeCall.ACTIVATE_WATCHDOG,
    NodeCall.DISABLE_WATCHDOG,
    NodeCall.KEEPALIVE_WATCHDOG,
    NodeCall.SLOT_CAPABILITIES,
))


class AgentRpc:
    """Validate one controller session and dispatch NodeControl calls."""

    def __init__(self, node: NodeControl, agent_boot_id: str, clock: Callable[[], float],
                 monitor: Any, policy_sink: Callable[[PolicyMode], None],
                 config_sink: Optional[Callable[[DynamicConfigPlan], ConfigApply]] = None) -> None:
        _uuid(agent_boot_id, 'agent boot ID')
        self._node = node
        self._agent_boot_id = agent_boot_id
        self._clock = clock
        self._monitor = monitor
        self._policy_sink = policy_sink
        self._config_sink = config_sink
        self._controller_boot_id: Optional[str] = None
        self._safety: Optional[SafetyState] = None
        self._last_sequence = 0
        self._history: 'OrderedDict[str, Tuple[bytes, Response]]' = OrderedDict()
        self._lock = RLock()
        self._safety_lock = RLock()
        self._fence_lock = Lock()
        self._timing: Optional[Timing] = None
        self._fence_count = 0
        self._fence_reason = FenceReason.NONE
        self._config_revision = 0
        self._config_fingerprint = ''

    def handle(self, request: object) -> Response:
        """Return a redacted response for one request."""
        try:
            with self._lock:
                if not isinstance(request, Request):
                    raise ProtocolError(ErrorCode.BAD_REQUEST, 'request envelope is invalid')
                return self._handle(request)
        except ProtocolError as exc:
            return Response(_request_id(request), exc.code, None)
        except ValidationError:
            return Response(_request_id(request), ErrorCode.FORBIDDEN, None)
        except (TypeError, ValueError):
            return Response(_request_id(request), ErrorCode.BAD_REQUEST, None)
        except Exception:
            logger.exception('Agent RPC failed')
            return Response(_request_id(request), ErrorCode.INTERNAL, None)

    def _handle(self, request: Request) -> Response:
        _uuid(request.request_id, 'request ID')
        _uuid(request.controller_boot_id, 'controller boot ID')
        if request.sequence < 1:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'sequence is invalid')

        fingerprint = hashlib.sha256(pack(request)).digest()
        previous = self._history.get(request.request_id)
        if previous:
            if previous[0] != fingerprint:
                raise ProtocolError(ErrorCode.CONFLICT, 'request ID conflict')
            return previous[1]
        is_rebind = request.operation == Operation.HELLO \
            and self._controller_boot_id is not None \
            and request.controller_boot_id != self._controller_boot_id
        if request.sequence <= self._last_sequence and not is_rebind:
            raise ProtocolError(ErrorCode.STALE, 'request sequence is stale')

        if request.operation == Operation.HELLO:
            body = self._hello(request)
        else:
            self._identity(request)
            body = self._dispatch(request)

        self._last_sequence = request.sequence
        response = Response(request.request_id, None, body)
        self._history[request.request_id] = (fingerprint, response)
        while len(self._history) > MAX_REQUEST_HISTORY:
            self._history.popitem(last=False)
        return response

    def _hello(self, request: Request) -> Hello:
        if request.agent_boot_id:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'hello contains agent identity')
        if request.body is not None:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'hello body is invalid')
        if self._controller_boot_id is None:
            self._controller_boot_id = request.controller_boot_id
            self._safety = SafetyState(
                self._agent_boot_id, request.controller_boot_id, self._clock, SAFETY_HISTORY,
            )
            self._monitor.bind(self._tick, self._fence, self._next_check)
            self._monitor.wake()
        elif request.controller_boot_id != self._controller_boot_id:
            with self._safety_lock:
                self._safety_state().rebind(request.controller_boot_id, request.sequence)
            self._controller_boot_id = request.controller_boot_id
            self._history.clear()
        snapshot = self._safety_state().snapshot
        return Hello(
            self._agent_boot_id,
            PROTOCOL_MAJOR,
            PROTOCOL_MINOR,
            CAPABILITIES,
            snapshot.authority_kind,
            snapshot.authority_term,
        )

    def _identity(self, request: Request) -> None:
        if request.controller_boot_id != self._controller_boot_id \
                or request.agent_boot_id != self._agent_boot_id:
            raise ProtocolError(ErrorCode.FORBIDDEN, 'session identity mismatch')

    def _dispatch(self, request: Request) -> object:
        if request.operation == Operation.SNAPSHOT:
            detail, freshness, context = _args(request.body, 3)
            return self._node.snapshot(detail, freshness, context)
        if request.operation == Operation.INVALIDATE:
            _none(request.body)
            self._node.invalidate()
            return None
        if request.operation == Operation.SUBMIT:
            return self._submit(request)
        if request.operation == Operation.COMMAND_STATUS:
            command_id, = _args(request.body, 1)
            return self._track(self._node.command_status(command_id))
        if request.operation == Operation.ACTIVE_COMMAND:
            _none(request.body)
            return self._track(self._node.active_command())
        if request.operation == Operation.COMMAND_WAIT:
            command_id, timeout = _args(request.body, 2)
            return self._track(self._node.command_wait(command_id, timeout))
        if request.operation == Operation.EVENTS:
            command_id, sequence, timeout = _args(request.body, 3)
            return self._node.command_events(command_id, sequence, timeout)
        if request.operation == Operation.ACK:
            command_id, sequence = _args(request.body, 2)
            self._node.command_ack(command_id, sequence)
            return None
        if request.operation == Operation.CANCEL:
            command_id, = _args(request.body, 1)
            return self._track(self._node.command_cancel(command_id))
        if request.operation == Operation.CALL:
            return self._call(request.body)
        if request.operation == Operation.GRANT:
            return self._grant(request)
        if request.operation == Operation.POLICY:
            return self._policy(request)
        if request.operation == Operation.FENCE:
            timeout, = _args(request.body, 1)
            with self._safety_lock:
                self._safety_state().fence()
            return self._fence(timeout, FenceReason.EXPLICIT)
        if request.operation == Operation.CONFIGURE:
            return self._configure(request.body)
        if request.operation == Operation.TELEMETRY:
            _none(request.body)
            return self._telemetry()
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'operation is not supported')

    def _submit(self, request: Request) -> CommandSubmission:
        command, authority_term = _args(request.body, 2)
        if not isinstance(command, LifecycleCommand):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'command is invalid')
        self._observe()
        with self._safety_lock:
            receipt = self._safety_state().submit(CommandRequest(
                command.command_id,
                request.controller_boot_id,
                request.agent_boot_id,
                request.sequence,
                command.kind,
                command.target_role,
                authority_term,
            ))
        if receipt.action == SafetyAction.FENCE:
            self._fence(reason=FenceReason.COMMAND)
            return CommandSubmission(SubmitState.REJECTED, None)
        if receipt.action == SafetyAction.REJECT:
            return CommandSubmission(SubmitState.REJECTED, None)
        return self._node.submit(command)

    def _track(self, result: Optional[CommandResult]) -> Optional[CommandResult]:
        if result is None or result.state == CommandState.RUNNING:
            return result
        action = SafetyAction.NONE
        with self._safety_lock:
            safety = self._safety_state()
            if safety.snapshot.active_command_id == result.request.command_id:
                action = safety.complete(result.request.command_id, result.state)
        if action == SafetyAction.FENCE:
            self._fence(reason=FenceReason.COMMAND)
        self._observe()
        return result

    def _grant(self, request: Request) -> None:
        body = request.body
        if not isinstance(body, AuthorityGrant) or body.sequence != request.sequence:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'authority grant is invalid')
        with self._safety_lock:
            self._safety_state().grant(body)
            self._timing = body.timing
        self._monitor.wake()

    def _policy(self, request: Request) -> SafetyAction:
        if not isinstance(request.body, PolicyMode):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'policy is invalid')
        with self._safety_lock:
            action = self._safety_state().policy(request.body, request.sequence)
        self._policy_sink(request.body)
        self._monitor.wake()
        if action == SafetyAction.FENCE:
            self._fence(reason=FenceReason.COMMAND)
        return action

    def _configure(self, body: object) -> ConfigApply:
        if not isinstance(body, DynamicConfigPlan) or self._config_sink is None:
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'dynamic configuration plan is invalid')
        result = self._config_sink(body)
        if not isinstance(cast(object, result), ConfigApply):
            raise ProtocolError(ErrorCode.INTERNAL, 'dynamic configuration result is invalid')
        self._config_revision = body.revision
        self._config_fingerprint = body.fingerprint

        return result

    def _telemetry(self) -> AgentTelemetry:
        active_kind = active_phase = None
        with self._safety_lock:
            safety = self._safety_state()
            command_id = safety.snapshot.active_command_id
            if command_id is not None:
                status = safety.command(command_id)
                active_kind = status.request.kind
                active_phase = status.phase

        return AgentTelemetry(
            active_kind,
            active_phase,
            self._fence_count,
            self._fence_reason,
            self._config_revision,
            self._config_fingerprint,
        )

    def _call(self, body: object) -> object:
        raw_call, raw_args = _args(body, 2)
        if not isinstance(raw_call, NodeCall) or not isinstance(raw_args, tuple):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'node call is invalid')
        args = cast(Tuple[Any, ...], raw_args)
        if raw_call in ZERO_NODE_CALLS:
            _zero(args)
        if raw_call == NodeCall.IS_PRIMARY:
            return self._node.is_primary()
        if raw_call == NodeCall.IS_RUNNING:
            return self._node.is_running()
        if raw_call == NodeCall.IS_STARTING:
            return self._node.is_starting()
        if raw_call == NodeCall.LAST_OPERATION:
            return self._node.last_operation()
        if raw_call == NodeCall.TIMELINE_WAL:
            return self._node.timeline_wal()
        if raw_call == NodeCall.REPLICATION_STATE:
            return self._node.replication_state()
        if raw_call == NodeCall.RECEIVED_TIMELINE:
            return self._node.received_timeline()
        if raw_call == NodeCall.REPLICA_TIMELINE:
            return self._node.replica_timeline(cast(Optional[int], _one(args)))
        if raw_call == NodeCall.CONTROL_TIMELINE:
            return self._node.control_timeline()
        if raw_call == NodeCall.POSTMASTER_START:
            return self._node.postmaster_start()
        if raw_call == NodeCall.SERVER_VERSION:
            return self._node.server_version()
        if raw_call == NodeCall.SLOTS:
            return self._node.slots()
        if raw_call == NodeCall.TIMELINE_HISTORY:
            return self._node.timeline_history(cast(int, _one(args)))
        if raw_call == NodeCall.CHECKPOINT_LOCATIONS:
            return self._node.checkpoint_locations()
        if raw_call == NodeCall.RECOVERY:
            return self._node.recovery()
        if raw_call == NodeCall.CAN_REWIND:
            return self._node.can_rewind()
        if raw_call == NodeCall.REWIND_NEEDED:
            return self._node.rewind_needed(cast(Optional[RecoveryTarget], _one(args)))
        if raw_call == NodeCall.ARCHIVE_READY:
            return self._node.archive_ready()
        if raw_call == NodeCall.CAN_CLONE:
            return self._node.can_clone(cast(Optional[Sequence[str]], _one(args)))
        if raw_call == NodeCall.DATA_EMPTY:
            return self._node.data_empty()
        if raw_call == NodeCall.CONTROLDATA:
            return self._node.controldata()
        if raw_call == NodeCall.RESTORED:
            return self._node.restored_from_backup()
        if raw_call == NodeCall.RECOVERY_CONF_EXISTS:
            return self._node.recovery_conf_exists()
        if raw_call == NodeCall.CHECK_RECOVERY_CONF:
            return self._node.check_recovery_conf(cast(Optional[RecoveryTarget], _one(args)))
        if raw_call == NodeCall.CANCEL_RECOVERY:
            self._node.cancel(cast(CancelMode, _one(args)))
            return None
        if raw_call == NodeCall.RESET_CANCEL:
            self._node.reset_cancel()
            return None
        if raw_call == NodeCall.WATCHDOG:
            return self._node.watchdog()
        if raw_call == NodeCall.ACTIVATE_WATCHDOG:
            return self._node.activate_watchdog()
        if raw_call == NodeCall.DISABLE_WATCHDOG:
            self._node.disable_watchdog()
            return None
        if raw_call == NodeCall.KEEPALIVE_WATCHDOG:
            self._node.keepalive_watchdog()
            return None
        if raw_call == NodeCall.RELOAD_WATCHDOG:
            return self._node.reload_watchdog(cast(WatchdogTiming, _one(args)))
        if raw_call == NodeCall.SYNC_STATE:
            return self._node.sync_state(cast(SyncContext, _one(args)))
        if raw_call == NodeCall.SLOT_CAPABILITIES:
            return self._node.slot_capabilities()
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'node call is not supported')

    def _tick(self) -> SafetyAction:
        with self._safety_lock:
            return self._safety_state().tick()

    def _next_check(self) -> Optional[float]:
        with self._safety_lock:
            return self._safety_state().next_check()

    def _observe(self) -> None:
        snapshot = self._node.snapshot(
            SnapshotDetail.BASIC, Freshness.FRESH, ObservationContext(None),
        )
        with self._safety_lock:
            action = self._safety_state().observe(snapshot.observed_role)
        self._monitor.wake()
        if action == SafetyAction.FENCE:
            self._fence()

    def _fence(self, timeout: Optional[float] = None,
               reason: FenceReason = FenceReason.AUTHORITY) -> bool:
        with self._fence_lock:
            self._fence_count += 1
            self._fence_reason = reason
            with self._safety_lock:
                timing = self._timing
            stop_timeout = timeout if timeout is not None \
                else timing.retry_timeout if timing is not None else DEFAULT_RPC_TIMEOUT
            stopped = self._node.fence(stop_timeout)
            snapshot = self._node.snapshot(
                SnapshotDetail.BASIC, Freshness.FRESH, ObservationContext(None),
            )
            if snapshot.observed_role == PostgresRole.PRIMARY:
                return stopped

            with self._safety_lock:
                safety = self._safety_state()
                if safety.snapshot.agent_state == AgentState.FENCING:
                    safety.fence_complete(snapshot.observed_role)
            return stopped

    def _safety_state(self) -> SafetyState:
        if self._safety is None:
            raise ProtocolError(ErrorCode.FORBIDDEN, 'hello is required')
        return self._safety


class AgentClient(NodeControl):
    """Use the NodeControl contract over one-request Unix connections."""

    def __init__(self, path: str, controller_boot_id: Optional[str] = None,
                 timeout: float = DEFAULT_RPC_TIMEOUT,
                 peer_check: Optional[Callable[[socket.socket], None]] = None) -> None:
        if timeout <= 0:
            raise ValueError('RPC timeout must be positive')
        self._path = path
        self._controller_boot_id = controller_boot_id or str(uuid4())
        _uuid(self._controller_boot_id, 'controller boot ID')
        self._agent_boot_id = ''
        self._timeout = timeout
        self._peer_check = peer_check or unix_peer_check()
        self._sequence = 0
        self._authority_term = 0
        self._closed = False
        self._connected = False
        self._last_success_at = 0.0
        self._last_snapshot_at = 0.0
        self._last_snapshot: Optional[NodeSnapshot] = None
        self._last_telemetry = AgentTelemetry(None, None, 0, FenceReason.NONE, 0, '')
        self._lock = RLock()
        self._stream: Optional[socket.socket] = None
        hello = self._rpc(Operation.HELLO, None)
        if not isinstance(hello, Hello):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'hello response is invalid')
        self._install_hello(hello)

    @property
    def agent_boot_id(self) -> str:
        """Return the negotiated agent process identity."""
        return self._agent_boot_id

    @property
    def controller_boot_id(self) -> str:
        """Return this controller process identity."""
        return self._controller_boot_id

    @property
    def capabilities(self) -> Tuple[Capability, ...]:
        """Return negotiated agent features."""
        return self._capabilities

    @property
    def authority_kind(self) -> Optional[AuthorityKind]:
        """Return authority inherited from a prior controller session."""
        return self._authority_kind

    @property
    def authority_term(self) -> int:
        """Return the last agent-accepted authority term."""
        return self._authority_term

    @property
    def connected(self) -> bool:
        """Return whether the last RPC reached the agent."""
        with self._lock:
            return self._connected

    @property
    def protocol_version(self) -> Tuple[int, int]:
        """Return the negotiated wire version."""
        return self._protocol_version

    @property
    def last_success_at(self) -> float:
        """Return the last successful RPC monotonic time."""
        with self._lock:
            return self._last_success_at

    @property
    def last_snapshot_at(self) -> float:
        """Return the last successful snapshot monotonic time."""
        with self._lock:
            return self._last_snapshot_at

    def snapshot(self, detail: SnapshotDetail, freshness: Freshness,
                 context: ObservationContext) -> NodeSnapshot:
        try:
            snapshot = cast(NodeSnapshot, self._rpc(Operation.SNAPSHOT, (detail, freshness, context)))
        except ProtocolError:
            if detail != SnapshotDetail.STATUS:
                raise
            return self._unavailable_snapshot(detail)
        with self._lock:
            self._last_snapshot_at = time.monotonic()
            self._last_snapshot = snapshot
        return snapshot

    def invalidate(self) -> None:
        self._rpc(Operation.INVALIDATE, None)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._connected = False
            self._close_stream()

    def submit(self, command: LifecycleCommand) -> CommandSubmission:
        return cast(CommandSubmission, self._rpc(Operation.SUBMIT, (command, self._authority_term)))

    def command_status(self, command_id: str) -> Optional[CommandResult]:
        return cast(Optional[CommandResult], self._rpc(Operation.COMMAND_STATUS, (command_id,)))

    def active_command(self) -> Optional[CommandResult]:
        return cast(Optional[CommandResult], self._rpc(Operation.ACTIVE_COMMAND, None))

    def command_wait(self, command_id: str, timeout: Optional[float]) -> Optional[CommandResult]:
        return cast(Optional[CommandResult], self._rpc(Operation.COMMAND_WAIT, (command_id, timeout)))

    def command_cancel(self, command_id: str) -> Optional[CommandResult]:
        return cast(Optional[CommandResult], self._rpc(Operation.CANCEL, (command_id,)))

    def command_events(self, command_id: str, after_sequence: int,
                       timeout: Optional[float] = None) -> Tuple[EventRecord, ...]:
        return cast(Tuple[EventRecord, ...], self._rpc(
            Operation.EVENTS, (command_id, after_sequence, timeout),
        ))

    def command_ack(self, command_id: str, sequence: int) -> None:
        self._rpc(Operation.ACK, (command_id, sequence))

    def is_primary(self) -> bool:
        return cast(bool, self._call(NodeCall.IS_PRIMARY))

    def is_running(self) -> bool:
        return cast(bool, self._call(NodeCall.IS_RUNNING))

    def is_starting(self) -> bool:
        return cast(bool, self._call(NodeCall.IS_STARTING))

    def last_operation(self) -> int:
        return cast(int, self._call(NodeCall.LAST_OPERATION))

    def timeline_wal(self) -> TimelineWal:
        return cast(TimelineWal, self._call(NodeCall.TIMELINE_WAL))

    def replication_state(self) -> Optional[str]:
        return cast(Optional[str], self._call(NodeCall.REPLICATION_STATE))

    def received_timeline(self) -> Optional[int]:
        return cast(Optional[int], self._call(NodeCall.RECEIVED_TIMELINE))

    def replica_timeline(self, leader_timeline: Optional[int]) -> Optional[int]:
        return cast(Optional[int], self._call(NodeCall.REPLICA_TIMELINE, leader_timeline))

    def control_timeline(self) -> Optional[int]:
        return cast(Optional[int], self._call(NodeCall.CONTROL_TIMELINE))

    def postmaster_start(self) -> Optional[str]:
        return cast(Optional[str], self._call(NodeCall.POSTMASTER_START))

    def server_version(self) -> int:
        return cast(int, self._call(NodeCall.SERVER_VERSION))

    def slots(self) -> Dict[str, int]:
        return cast(Dict[str, int], self._call(NodeCall.SLOTS))

    def timeline_history(self, timeline: int) -> Tuple[Tuple[object, ...], ...]:
        return cast(Tuple[Tuple[object, ...], ...], self._call(NodeCall.TIMELINE_HISTORY, timeline))

    def checkpoint_locations(self) -> Tuple[Optional[int], Optional[int]]:
        return cast(Tuple[Optional[int], Optional[int]], self._call(NodeCall.CHECKPOINT_LOCATIONS))

    def recovery(self) -> RecoverySnapshot:
        return cast(RecoverySnapshot, self._call(NodeCall.RECOVERY))

    def can_rewind(self) -> bool:
        return cast(bool, self._call(NodeCall.CAN_REWIND))

    def rewind_needed(self, target: Optional[RecoveryTarget]) -> bool:
        return cast(bool, self._call(NodeCall.REWIND_NEEDED, target))

    def archive_ready(self) -> bool:
        return cast(bool, self._call(NodeCall.ARCHIVE_READY))

    def can_clone(self, methods: Optional[Sequence[str]]) -> bool:
        return cast(bool, self._call(NodeCall.CAN_CLONE, tuple(methods) if methods is not None else None))

    def data_empty(self) -> bool:
        return cast(bool, self._call(NodeCall.DATA_EMPTY))

    def controldata(self) -> Dict[str, str]:
        return cast(Dict[str, str], self._call(NodeCall.CONTROLDATA))

    def restored_from_backup(self) -> bool:
        return cast(bool, self._call(NodeCall.RESTORED))

    def recovery_conf_exists(self) -> bool:
        return cast(bool, self._call(NodeCall.RECOVERY_CONF_EXISTS))

    def check_recovery_conf(self, target: Optional[RecoveryTarget]) -> ConfigChange:
        return cast(ConfigChange, self._call(NodeCall.CHECK_RECOVERY_CONF, target))

    def cancel(self, mode: CancelMode) -> None:
        self._call(NodeCall.CANCEL_RECOVERY, mode)

    def reset_cancel(self) -> None:
        self._call(NodeCall.RESET_CANCEL)

    def fence(self, timeout: Optional[float]) -> bool:
        return cast(bool, self._rpc(Operation.FENCE, (timeout,)))

    def watchdog(self) -> WatchdogSnapshot:
        return cast(WatchdogSnapshot, self._call(NodeCall.WATCHDOG))

    def activate_watchdog(self) -> bool:
        return cast(bool, self._call(NodeCall.ACTIVATE_WATCHDOG))

    def disable_watchdog(self) -> None:
        self._call(NodeCall.DISABLE_WATCHDOG)

    def keepalive_watchdog(self) -> None:
        self._call(NodeCall.KEEPALIVE_WATCHDOG)

    def reload_watchdog(self, timing: WatchdogTiming) -> WatchdogReload:
        return cast(WatchdogReload, self._call(NodeCall.RELOAD_WATCHDOG, timing))

    def sync_state(self, context: SyncContext) -> SyncSnapshot:
        return cast(SyncSnapshot, self._call(NodeCall.SYNC_STATE, context))

    def slot_capabilities(self) -> SlotCapabilities:
        return cast(SlotCapabilities, self._call(NodeCall.SLOT_CAPABILITIES))

    def grant(self, grant: AuthorityGrant) -> None:
        self._rpc(Operation.GRANT, grant)
        self._authority_kind = grant.kind
        self._authority_term = grant.term

    def configure(self, plan: DynamicConfigPlan) -> ConfigApply:
        return cast(ConfigApply, self._rpc(Operation.CONFIGURE, plan))

    def telemetry(self) -> AgentTelemetry:
        try:
            telemetry = cast(AgentTelemetry, self._rpc(Operation.TELEMETRY, None))
        except ProtocolError:
            return self._last_telemetry
        with self._lock:
            self._last_telemetry = telemetry
        return telemetry

    def _unavailable_snapshot(self, detail: SnapshotDetail) -> NodeSnapshot:
        with self._lock:
            previous = self._last_snapshot
        return NodeSnapshot(
            self._agent_boot_id,
            previous.sequence if previous is not None else 0,
            previous.collected_at if previous is not None else 0.0,
            detail,
            PostgresRole.UNKNOWN,
            previous.desired_role if previous is not None else PostgresRole.UNKNOWN,
            PostgresState.UNKNOWN,
            previous.supports_multiple_sync if previous is not None else False,
            previous.system_identifier if previous is not None else '',
            0,
            None,
            WalObservation(None, None, None, None, None),
            None,
            None,
            (),
            None,
            previous.pending_restart if previous is not None else (),
            ObservationFailure.AGENT_UNAVAILABLE,
        )

    def policy(self, mode: PolicyMode) -> SafetyAction:
        return cast(SafetyAction, self._rpc(Operation.POLICY, mode))

    def _call(self, call: NodeCall, *args: object) -> object:
        return self._rpc(Operation.CALL, (call, tuple(args)))

    def _rpc(self, operation: Operation, body: object) -> object:
        with self._lock:
            if self._closed:
                raise ProtocolError(ErrorCode.UNAVAILABLE, 'agent client is closed')
            response = self._send(operation, body)
            if response.error == ErrorCode.FORBIDDEN and operation != Operation.HELLO:
                self._reconnect()
                if operation == Operation.SUBMIT:
                    raise ProtocolError(ErrorCode.UNAVAILABLE, 'agent restarted before command submission')
                response = self._send(operation, body)
        if response.error is not None:
            raise ProtocolError(response.error, 'agent request failed')
        return response.body

    def _send(self, operation: Operation, body: object) -> Response:
        self._sequence += 1
        if operation == Operation.GRANT:
            if not isinstance(body, AuthorityGrant):
                raise ProtocolError(ErrorCode.BAD_REQUEST, 'authority grant is invalid')
            body = body._replace(
                controller_boot_id=self._controller_boot_id,
                agent_boot_id=self._agent_boot_id,
                sequence=self._sequence,
            )
        agent_boot_id = '' if operation == Operation.HELLO else self._agent_boot_id
        request = Request(
            str(uuid4()),
            operation,
            self._controller_boot_id,
            agent_boot_id,
            self._sequence,
            body,
        )
        return self._exchange(request)

    def _reconnect(self) -> None:
        response = self._send(Operation.HELLO, None)
        if response.error is not None or not isinstance(response.body, Hello):
            raise ProtocolError(ErrorCode.UNAVAILABLE, 'agent handshake failed')
        self._install_hello(response.body)

    def _install_hello(self, hello: Hello) -> None:
        _hello(hello)
        if self._agent_boot_id and hello.agent_boot_id != self._agent_boot_id:
            self._last_snapshot_at = 0.0
            self._last_snapshot = None
            self._last_telemetry = AgentTelemetry(None, None, 0, FenceReason.NONE, 0, '')
        self._agent_boot_id = hello.agent_boot_id
        self._capabilities = hello.capabilities
        self._protocol_version = hello.protocol_major, hello.protocol_minor
        self._authority_kind = hello.authority_kind
        self._authority_term = hello.authority_term

    def _exchange(self, request: Request) -> Response:
        last_error: Optional[Exception] = None
        for _ in range(MAX_RPC_ATTEMPTS):
            try:
                stream = self._stream or self._connect()
                write_frame(stream, request)
                response = read_frame(stream)
                if not isinstance(response, Response) or response.request_id != request.request_id:
                    raise ProtocolError(ErrorCode.BAD_REQUEST, 'response envelope is invalid')
                self._connected = True
                self._last_success_at = time.monotonic()
                return response
            except (OSError, ProtocolError) as exc:
                last_error = exc
                self._close_stream()
        self._connected = False
        raise ProtocolError(ErrorCode.UNAVAILABLE, 'agent is unavailable') from last_error

    def _connect(self) -> socket.socket:
        stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            stream.settimeout(self._timeout)
            stream.connect(self._path)
            self._peer_check(stream)
        except Exception:
            stream.close()
            raise
        self._stream = stream
        return stream

    def _close_stream(self) -> None:
        stream = self._stream
        self._stream = None
        if stream is not None:
            stream.close()


def _args(value: object, count: int) -> Tuple[Any, ...]:
    if not isinstance(value, tuple):
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'operation arguments are invalid')
    args = cast(Tuple[Any, ...], value)
    if len(args) != count:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'operation arguments are invalid')
    return args


def _one(args: Tuple[Any, ...]) -> Any:
    if len(args) != 1:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'node call arguments are invalid')
    return args[0]


def _zero(args: Tuple[Any, ...]) -> None:
    if args:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'node call arguments are invalid')


def _none(value: object) -> None:
    if value is not None:
        raise ProtocolError(ErrorCode.BAD_REQUEST, 'operation body is invalid')


def _uuid(value: object, field: str) -> None:
    try:
        valid = isinstance(value, str) and value == str(UUID(value))
    except (AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ProtocolError(ErrorCode.BAD_REQUEST, '{0} is invalid'.format(field))


def _hello(value: Hello) -> None:
    _uuid(value.agent_boot_id, 'agent boot ID')
    if value.protocol_major != PROTOCOL_MAJOR or value.protocol_minor != PROTOCOL_MINOR:
        raise ProtocolError(ErrorCode.VERSION, 'protocol version mismatch')
    if not value.capabilities or len(set(value.capabilities)) != len(value.capabilities):
        raise ProtocolError(ErrorCode.VERSION, 'protocol capabilities are invalid')
    if any(not isinstance(cast(object, capability), Capability) for capability in value.capabilities):
        raise ProtocolError(ErrorCode.VERSION, 'protocol capabilities are invalid')
    if value.authority_kind is not None \
            and not isinstance(cast(object, value.authority_kind), AuthorityKind):
        raise ProtocolError(ErrorCode.VERSION, 'authority kind is invalid')
    if not isinstance(cast(object, value.authority_term), int) or isinstance(value.authority_term, bool) \
            or value.authority_term < 0:
        raise ProtocolError(ErrorCode.VERSION, 'authority term is invalid')
    if (value.authority_kind is None) != (value.authority_term == 0):
        raise ProtocolError(ErrorCode.VERSION, 'authority state is invalid')


def _request_id(request: object) -> str:
    if not isinstance(request, Request):
        return ''
    request_id = cast(object, request.request_id)
    return request_id if isinstance(request_id, str) else ''
