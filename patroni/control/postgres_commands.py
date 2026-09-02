"""PostgreSQL lifecycle command driver."""
from threading import Event, RLock
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from patroni import global_config
from patroni.async_executor import CriticalTask
from patroni.dcs import Member, RemoteMember
from patroni.postgresql.misc import PostgresqlRole
from patroni.postgresql.postmaster import PostmasterProcess

from .commands import CheckpointMode, CommandDriver, CommandValue, DivergencePolicy, DriverResult, \
    EventChannel, EventKind, FollowTarget, LifecycleCommand, ReloadMode, SlotMode, StopMode, TargetKind
from .models import CommandKind, DesiredRole
from .recovery import PostgresRecovery

if TYPE_CHECKING:  # pragma: no cover
    from .replication import PostgresReplication

if TYPE_CHECKING:  # pragma: no cover
    from patroni.postgresql import Postgresql


DEFAULT_EVENT_TIMEOUT = 30.0


class PostgresCommandDriver(CommandDriver):
    """Translate typed lifecycle commands into PostgreSQL operations."""

    def __init__(self, postgresql: 'Postgresql', recovery: Optional[PostgresRecovery] = None,
                 replication: Optional['PostgresReplication'] = None) -> None:
        self._postgresql = postgresql
        self._recovery = recovery or PostgresRecovery(postgresql, lambda: None)
        self._replication = replication
        self._lock = RLock()
        self._task: Optional[CriticalTask] = None
        self._checkpoint_location: Optional[int] = None
        self._previous_location: Optional[int] = None
        self._output = ()

    def run(self, command: LifecycleCommand, events: EventChannel,
            cancelled: Event) -> DriverResult:
        task = CriticalTask()
        with self._lock:
            self._task = task
            self._checkpoint_location = None
            self._previous_location = None
            self._output = ()

        self._postgresql.cancellable.reset_is_cancelled()
        try:
            value = self._dispatch(command, events, cancelled, task)
            with self._lock:
                return DriverResult(_value(value), self._checkpoint_location, self._previous_location, self._output)
        finally:
            with self._lock:
                self._task = None

    def cancel(self) -> None:
        postmaster = None
        with self._lock:
            task = self._task
        if task:
            with task:
                if not task.cancel() and isinstance(task.result, PostmasterProcess):
                    postmaster = task.result

        self._postgresql.cancellable.cancel()
        if postmaster:
            self._postgresql.terminate_starting_postmaster(postmaster)

    def fence(self, timeout: Optional[float]) -> bool:
        self.cancel()
        stop_timeout = int(timeout) if timeout is not None else None
        return bool(self._postgresql.stop('immediate', checkpoint=False, stop_timeout=stop_timeout))

    def _dispatch(self, command: LifecycleCommand, events: EventChannel,
                  cancelled: Event, task: CriticalTask) -> Optional[bool]:
        role = _role(command.target_role)
        if command.kind == CommandKind.START:
            after_start = self._callback(command, events, cancelled, EventKind.AFTER_START)
            return self._postgresql.start(command.timeout, task, role=role, after_start=after_start)
        if command.kind == CommandKind.RESTART:
            before_shutdown = self._callback(command, events, cancelled, EventKind.BEFORE_SHUTDOWN)
            after_start = self._callback(command, events, cancelled, EventKind.AFTER_START)
            return self._postgresql.restart(
                command.timeout,
                task,
                role=role,
                before_shutdown=before_shutdown,
                after_start=after_start,
            )
        if command.kind == CommandKind.PROMOTE:
            self._recovery.reset()
            before_promote = self._callback(command, events, cancelled, EventKind.BEFORE_PROMOTE)
            return self._postgresql.promote(int(command.timeout or 0), task, before_promote)
        if command.kind == CommandKind.STOP:
            return self._stop(command, events, cancelled)
        if command.kind == CommandKind.FOLLOW:
            role = role or PostgresqlRole.REPLICA
            member = _member(command.follow_target)
            if command.reload == ReloadMode.RELOAD:
                return self._postgresql.follow(member, role, do_reload=True)
            if command.timeout is not None:
                return self._postgresql.follow(member, role, command.timeout)

            return self._postgresql.follow(member, role)
        if command.kind == CommandKind.FENCE:
            timeout = int(command.timeout) if command.timeout is not None else None
            return self._postgresql.stop('immediate', checkpoint=False, stop_timeout=timeout)
        if command.kind == CommandKind.BOOTSTRAP:
            self._recovery.set_bootstrapping(command.bootstrap_state)
            return self._recovery.bootstrap()
        if command.kind == CommandKind.CLONE:
            return self._clone(command)
        if command.kind == CommandKind.REWIND:
            if command.divergence != DivergencePolicy.REWIND or command.recovery_target is None:
                raise ValueError('invalid rewind policy')
            return self._recovery.execute(command.recovery_target)
        if command.kind == CommandKind.CRASH_RECOVERY:
            return self._recovery.clean_shutdown()
        if command.kind == CommandKind.POST_BOOTSTRAP:
            return self._recovery.post_bootstrap()
        if command.kind == CommandKind.REINITIALIZE:
            if command.divergence != DivergencePolicy.REINITIALIZE:
                raise ValueError('invalid reinitialize policy')
            self._postgresql.stop('immediate', stop_timeout=int(command.timeout or 0))
            return self._clone(command)
        if command.kind == CommandKind.APPLY_CONFIG:
            self._recovery.apply_parameters()
            return True
        if command.kind == CommandKind.APPLY_SYNC:
            if command.sync_plan is None or self._replication is None:
                raise ValueError('sync plan is required')
            self._replication.apply_sync(command.sync_plan)
            return True
        if command.kind in (CommandKind.APPLY_SLOTS, CommandKind.COPY_SLOTS):
            if command.slot_plan is None or self._replication is None:
                raise ValueError('slot plan is required')
            self._output = self._replication.apply_slots(command.slot_plan)
            return True
        if command.kind == CommandKind.CALLBACK:
            if command.callback is None:
                raise ValueError('callback kind is required')
            self._recovery.callback(command.callback)
            return True
        if command.kind == CommandKind.REMOVE_DATA:
            self._recovery.remove_data()
            return True
        if command.kind == CommandKind.MOVE_DATA:
            self._recovery.move_data()
            return True
        if command.kind == CommandKind.SET_BOOTSTRAP:
            self._recovery.set_bootstrapping(command.bootstrap_state)
            return True
        if command.kind == CommandKind.RESET_RECOVERY:
            self._recovery.reset()
            return True
        if command.kind == CommandKind.CHECK_DIVERGENCE:
            self._recovery.trigger()
            return True
        if command.kind == CommandKind.CHECKPOINT:
            self._recovery.ensure_checkpoint()
            return True
        if command.kind == CommandKind.ARCHIVE_WAL:
            self._recovery.archive_shutdown()
            return True

        raise ValueError('unsupported lifecycle command')

    def _clone(self, command: LifecycleCommand) -> bool:
        self._recovery.reset()
        result = self._recovery.clone(command.recovery_target, command.clone_mode)
        if not result:
            self._recovery.remove_data()

        return result

    def _stop(self, command: LifecycleCommand, events: EventChannel,
              cancelled: Event) -> bool:
        kwargs: Dict[str, Any] = {}
        if command.checkpoint != CheckpointMode.DEFAULT:
            kwargs['checkpoint'] = _checkpoint(command.checkpoint)
        if command.timeout is not None:
            kwargs['stop_timeout'] = int(command.timeout)

        callbacks = (
            ('on_safepoint', EventKind.SAFEPOINT),
            ('before_shutdown', EventKind.BEFORE_SHUTDOWN),
        )
        for name, kind in callbacks:
            callback = self._callback(command, events, cancelled, kind)
            if callback:
                kwargs[name] = callback
        shutdown_callback = self._shutdown_callback(command, events, cancelled)
        if shutdown_callback:
            kwargs['on_shutdown'] = shutdown_callback

        if command.stop_mode == StopMode.DEFAULT:
            return self._postgresql.stop(**kwargs)

        return self._postgresql.stop(command.stop_mode.value, **kwargs)

    def _callback(self, command: LifecycleCommand, events: EventChannel,
                  cancelled: Event, kind: EventKind) -> Optional[Callable[[], None]]:
        if kind not in command.events:
            return None

        def callback() -> None:
            event = events.publish(kind)
            timeout = command.timeout if command.timeout is not None else DEFAULT_EVENT_TIMEOUT
            events.wait_ack(event.sequence, timeout, cancelled)

        return callback

    def _shutdown_callback(self, command: LifecycleCommand, events: EventChannel,
                           cancelled: Event) -> Optional[Callable[[int, int], None]]:
        if EventKind.SHUTDOWN not in command.events:
            return None

        def callback(checkpoint_location: int, previous_location: int) -> None:
            with self._lock:
                self._checkpoint_location = checkpoint_location
                self._previous_location = previous_location
            event = events.publish(EventKind.SHUTDOWN, checkpoint_location, previous_location)
            timeout = command.timeout if command.timeout is not None else DEFAULT_EVENT_TIMEOUT
            events.wait_ack(event.sequence, timeout, cancelled)

        return callback


def _checkpoint(mode: CheckpointMode) -> Optional[bool]:
    if mode == CheckpointMode.DEFAULT:
        return None

    return mode == CheckpointMode.ENABLED


def _role(role: DesiredRole) -> Optional[PostgresqlRole]:
    if role == DesiredRole.UNCHANGED:
        return None
    if role == DesiredRole.PRIMARY:
        return PostgresqlRole.PRIMARY
    if role == DesiredRole.STANDBY_LEADER:
        return PostgresqlRole.STANDBY_LEADER

    return PostgresqlRole.REPLICA


def _value(value: object) -> CommandValue:
    if value is True:
        return CommandValue.TRUE
    if value is False:
        return CommandValue.FALSE
    if value is None:
        return CommandValue.PENDING

    raise ValueError('invalid PostgreSQL command result')


def _member(target: Optional[FollowTarget]) -> Optional[Member]:
    if target is None:
        return None

    data: Dict[str, Any] = {}
    if target.host:
        data['conn_kwargs'] = {
            'host': target.host,
            'port': target.port,
            'dbname': target.database,
        }
    if target.kind == TargetKind.REMOTE:
        standby_config = global_config.get_standby_cluster_config()
        recovery_parameters = ('restore_command', 'archive_cleanup_command', 'recovery_min_apply_delay')
        data.update({name: standby_config[name] for name in recovery_parameters if standby_config.get(name)})
        data['no_replication_slot'] = target.slot_mode == SlotMode.DISABLE
        if target.slot_name:
            data['primary_slot_name'] = target.slot_name
        return RemoteMember(target.name, data)

    return Member(-1, target.name, None, data)
