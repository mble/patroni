"""Agent-local PostgreSQL recovery driver."""
from copy import deepcopy
from typing import Any, Callable, cast, Dict, Mapping, Optional, Sequence, Union

from patroni import global_config
from patroni.async_executor import CriticalTask
from patroni.dcs import Leader, Member, RemoteMember
from patroni.postgresql.callback_executor import CallbackAction
from patroni.postgresql.rewind import Rewind

from .commands import BootstrapState, CallbackKind, CancelMode, CloneMode, RecoveryTarget, SlotMode, TargetKind
from .models import ConfigChange, RecoverySnapshot

BOOTSTRAP_DCS_KEY = 'dcs'
REMOTE_RECOVERY_PARAMETERS = (
    'restore_command',
    'archive_cleanup_command',
    'recovery_min_apply_delay',
    'create_replica_methods',
)


class PostgresRecovery:
    """Keep rewind state and credentials below the node boundary."""

    def __init__(self, postgresql: Any, wakeup: Callable[..., Any],
                 bootstrap_config: Optional[Mapping[str, Any]] = None) -> None:
        self._postgresql = postgresql
        self._rewind = Rewind(postgresql)
        self._wakeup = wakeup
        self._archive_command: Optional[str] = None
        self._bootstrap_config = deepcopy({
            key: value for key, value in (bootstrap_config or {}).items() if key != BOOTSTRAP_DCS_KEY
        })

    def snapshot(self) -> RecoverySnapshot:
        return RecoverySnapshot(
            self._rewind.is_needed,
            self._rewind.executed,
            self._rewind.failed,
            self._rewind.checkpoint_after_promote(),
            self._rewind.should_remove_data_directory_on_diverged_timelines,
            bool(self._postgresql.bootstrapping),
            bool(self._postgresql.cb_called),
        )

    def can_rewind(self) -> bool:
        return self._rewind.can_rewind

    def reset(self) -> None:
        self._rewind.reset_state()

    def trigger(self) -> None:
        self._rewind.trigger_check_diverged_lsn()

    def needed(self, target: Optional[RecoveryTarget]) -> bool:
        return self._rewind.rewind_or_reinitialize_needed_and_possible(_target(target))

    def execute(self, target: RecoveryTarget) -> Optional[bool]:
        source = _target(target)
        if source is None:
            raise ValueError('rewind source is required')

        return self._rewind.execute(source)

    def clean_shutdown(self) -> Optional[bool]:
        return self._rewind.ensure_clean_shutdown()

    def ensure_checkpoint(self) -> None:
        self._rewind.ensure_checkpoint_after_promote(self._wakeup)

    def archive_enabled(self) -> bool:
        self._archive_command = self._rewind.get_archive_command()
        return self._archive_command is not None

    def archive_shutdown(self) -> None:
        command = self._archive_command
        self._archive_command = None
        if command is not None:
            self._rewind.archive_shutdown_checkpoint_wal(command)

    def bootstrap(self) -> bool:
        return self._postgresql.bootstrap.bootstrap(self._bootstrap_config)

    def clone(self, target: Optional[RecoveryTarget], mode: CloneMode) -> bool:
        return self._postgresql.bootstrap.clone(_target(target), mode == CloneMode.LEADER)

    def can_clone(self, methods: Optional[Sequence[str]]) -> bool:
        return self._postgresql.can_create_replica_without_replication_connection(methods)

    def post_bootstrap(self) -> Optional[bool]:
        task = CriticalTask()
        return self._postgresql.bootstrap.post_bootstrap(self._bootstrap_config, task)

    def set_bootstrapping(self, state: BootstrapState) -> None:
        self._postgresql.bootstrapping = state == BootstrapState.RUNNING

    def remove_data(self) -> None:
        self._postgresql.remove_data_directory()

    def move_data(self) -> None:
        self._postgresql.move_data_directory()

    def data_empty(self) -> bool:
        return self._postgresql.data_directory_empty()

    def controldata(self) -> Dict[str, str]:
        return self._postgresql.controldata()

    def restored(self) -> bool:
        return self._postgresql.was_restored_from_backup()

    def recovery_conf_exists(self) -> bool:
        return self._postgresql.config.recovery_conf_exists()

    def check_recovery_conf(self, target: Optional[RecoveryTarget]) -> ConfigChange:
        change, restart = self._postgresql.config.check_recovery_conf(_target(target))
        return ConfigChange(change, restart)

    def apply_parameters(self) -> None:
        self._postgresql.handle_parameter_change()

    def refresh_sync_config(self) -> None:
        value = self._postgresql.config.synchronous_standby_names
        self._postgresql.config.set_synchronous_standby_names(value)

    def callback(self, kind: CallbackKind) -> None:
        action = {
            CallbackKind.START: CallbackAction.ON_START,
            CallbackKind.ROLE_CHANGE: CallbackAction.ON_ROLE_CHANGE,
        }[kind]
        self._postgresql.call_nowait(action)

    def cancel(self, mode: CancelMode) -> None:
        self._postgresql.cancellable.cancel(mode == CancelMode.KILL)

    def reset_cancel(self) -> None:
        self._postgresql.cancellable.reset_is_cancelled()


def _target(target: Optional[RecoveryTarget]) -> Optional[Union[Leader, RemoteMember]]:
    if target is None:
        return None

    data: Dict[str, Any] = {
        'conn_kwargs': {
            'host': target.host,
            'port': target.port,
            'dbname': target.database,
        },
    }
    if target.kind == TargetKind.REMOTE:
        raw_standby = global_config.get_standby_cluster_config()
        if isinstance(raw_standby, Mapping) and raw_standby:
            standby = cast(Mapping[str, Any], raw_standby)
            data.update({key: standby[key] for key in REMOTE_RECOVERY_PARAMETERS if standby.get(key)})
            data['no_replication_slot'] = target.slot_mode == SlotMode.DISABLE
            if target.slot_name:
                data['primary_slot_name'] = target.slot_name
            return RemoteMember(target.name, data)

    data['role'] = target.role
    if target.checkpoint_after_promote is None:
        data['version'] = '1.5.6'
    else:
        data['version'] = '2.0.0'
        if not target.checkpoint_after_promote:
            data['checkpoint_after_promote'] = False
    member = Member(-1, target.name, None, data)
    return Leader(-1, None, member)
