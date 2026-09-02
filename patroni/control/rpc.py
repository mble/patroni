"""NodeControl RPC service and controller client."""
import hashlib
import logging
import socket

from collections import OrderedDict
from threading import Lock, RLock
from typing import Any, Callable, cast, Dict, Optional, Sequence, Tuple
from uuid import UUID, uuid4

from .commands import CancelMode, CommandResult, CommandSubmission, \
    EventRecord, LifecycleCommand, RecoveryTarget, SubmitState
from .models import AgentState, AuthorityGrant, CommandRequest, CommandState, ConfigChange, Freshness, NodeSnapshot, \
    ObservationContext, PolicyMode, PostgresRole, RecoverySnapshot, SafetyAction, SlotCapabilities, SnapshotDetail, \
    SyncContext, SyncSnapshot, TimelineWal, Timing, WatchdogReload, WatchdogSnapshot, WatchdogTiming
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
                 monitor: Any, policy_sink: Callable[[PolicyMode], None]) -> None:
        _uuid(agent_boot_id, 'agent boot ID')
        self._node = node
        self._agent_boot_id = agent_boot_id
        self._clock = clock
        self._monitor = monitor
        self._policy_sink = policy_sink
        self._controller_boot_id: Optional[str] = None
        self._safety: Optional[SafetyState] = None
        self._last_sequence = 0
        self._history: 'OrderedDict[str, Tuple[bytes, Response]]' = OrderedDict()
        self._lock = RLock()
        self._safety_lock = RLock()
        self._fence_lock = Lock()
        self._timing: Optional[Timing] = None

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
        if request.sequence <= self._last_sequence:
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
        if self._controller_boot_id is not None and request.controller_boot_id != self._controller_boot_id:
            raise ProtocolError(ErrorCode.CONFLICT, 'controller identity changed')
        if self._controller_boot_id is None:
            self._controller_boot_id = request.controller_boot_id
            self._safety = SafetyState(
                self._agent_boot_id, request.controller_boot_id, self._clock, SAFETY_HISTORY,
            )
            self._monitor.bind(self._tick, self._fence)
            self._monitor.wake()
        return Hello(
            self._agent_boot_id, PROTOCOL_MAJOR, PROTOCOL_MINOR, CAPABILITIES,
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
            return self._fence(timeout)
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
            self._fence()
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
            self._fence()
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
            self._fence()
        return action

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

    def _observe(self) -> None:
        snapshot = self._node.snapshot(
            SnapshotDetail.BASIC, Freshness.FRESH, ObservationContext(None),
        )
        with self._safety_lock:
            action = self._safety_state().observe(snapshot.observed_role)
        if action == SafetyAction.FENCE:
            self._fence()

    def _fence(self, timeout: Optional[float] = None) -> bool:
        with self._fence_lock:
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
        self._lock = RLock()
        hello = self._rpc(Operation.HELLO, None)
        if not isinstance(hello, Hello):
            raise ProtocolError(ErrorCode.BAD_REQUEST, 'hello response is invalid')
        _hello(hello)
        self._agent_boot_id = hello.agent_boot_id
        self._capabilities = hello.capabilities

    @property
    def agent_boot_id(self) -> str:
        """Return the negotiated agent process identity."""
        return self._agent_boot_id

    @property
    def capabilities(self) -> Tuple[Capability, ...]:
        """Return negotiated agent features."""
        return self._capabilities

    def snapshot(self, detail: SnapshotDetail, freshness: Freshness,
                 context: ObservationContext) -> NodeSnapshot:
        return cast(NodeSnapshot, self._rpc(Operation.SNAPSHOT, (detail, freshness, context)))

    def invalidate(self) -> None:
        self._rpc(Operation.INVALIDATE, None)

    def close(self) -> None:
        self._closed = True

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
        self._authority_term = grant.term

    def policy(self, mode: PolicyMode) -> SafetyAction:
        return cast(SafetyAction, self._rpc(Operation.POLICY, mode))

    def _call(self, call: NodeCall, *args: object) -> object:
        return self._rpc(Operation.CALL, (call, tuple(args)))

    def _rpc(self, operation: Operation, body: object) -> object:
        with self._lock:
            if self._closed:
                raise ProtocolError(ErrorCode.UNAVAILABLE, 'agent client is closed')
            self._sequence += 1
            if operation == Operation.GRANT:
                if not isinstance(body, AuthorityGrant):
                    raise ProtocolError(ErrorCode.BAD_REQUEST, 'authority grant is invalid')
                body = body._replace(sequence=self._sequence)
            request = Request(
                str(uuid4()),
                operation,
                self._controller_boot_id,
                self._agent_boot_id,
                self._sequence,
                body,
            )
            response = self._exchange(request)
        if response.error is not None:
            raise ProtocolError(response.error, 'agent request failed')
        return response.body

    def _exchange(self, request: Request) -> Response:
        last_error: Optional[Exception] = None
        for _ in range(MAX_RPC_ATTEMPTS):
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
                    stream.settimeout(self._timeout)
                    stream.connect(self._path)
                    self._peer_check(stream)
                    write_frame(stream, request)
                    response = read_frame(stream)
                if not isinstance(response, Response) or response.request_id != request.request_id:
                    raise ProtocolError(ErrorCode.BAD_REQUEST, 'response envelope is invalid')
                return response
            except (OSError, ProtocolError) as exc:
                last_error = exc
        raise ProtocolError(ErrorCode.UNAVAILABLE, 'agent is unavailable') from last_error


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
    if value.protocol_major != PROTOCOL_MAJOR or value.protocol_minor > PROTOCOL_MINOR:
        raise ProtocolError(ErrorCode.VERSION, 'protocol version mismatch')
    if not value.capabilities or len(set(value.capabilities)) != len(value.capabilities):
        raise ProtocolError(ErrorCode.VERSION, 'protocol capabilities are invalid')
    if any(not isinstance(cast(object, capability), Capability) for capability in value.capabilities):
        raise ProtocolError(ErrorCode.VERSION, 'protocol capabilities are invalid')


def _request_id(request: object) -> str:
    if not isinstance(request, Request):
        return ''
    request_id = cast(object, request.request_id)
    return request_id if isinstance(request_id, str) else ''
