"""Bounded agent command-result journal."""
import hashlib
import json
import os
import stat

from collections import OrderedDict
from typing import Any, cast, Dict, List, NamedTuple, Optional, Tuple
from uuid import UUID, uuid4

from .commands import CommandResult, CommandValue, FollowTarget, LifecycleCommand, RecoveryTarget, SlotPlan, SyncPlan
from .models import CommandState

JOURNAL_FILE = 'commands.json'
JOURNAL_VERSION = 2
MAX_JOURNAL_BYTES = 4 * 1024 * 1024
MAX_JOURNAL_ENTRIES = 4096
FINGERPRINT_LENGTH = 64
FILE_MODE = 0o600
UNSAFE_DIRECTORY_BITS = stat.S_IWGRP | stat.S_IWOTH
UNSAFE_FILE_BITS = stat.S_IRWXG | stat.S_IRWXO
DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0)
READ_FLAGS = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)


class JournalError(ValueError):
    """Journal is unsafe, corrupt, or incompatible."""


class _Stored(NamedTuple):
    fingerprint: str
    state: CommandState
    value: CommandValue
    checkpoint_location: Optional[int]
    previous_location: Optional[int]
    output: Tuple[str, ...]


class CommandJournal:
    """Persist only terminal identity hashes and public outcomes."""

    def __init__(self, directory: str) -> None:
        if not os.path.isabs(directory) or os.path.normpath(directory) != directory:
            raise JournalError('journal directory must be canonical and absolute')
        self._directory = directory
        self._entries: 'OrderedDict[str, _Stored]' = OrderedDict()
        self._load()

    def get(self, request: LifecycleCommand) -> Optional[CommandResult]:
        stored = self._entries.get(request.command_id)
        if stored is None:
            return None
        if stored.fingerprint != _fingerprint(request):
            raise JournalError('conflicting command ID')

        return CommandResult(
            request,
            stored.state,
            stored.value,
            stored.checkpoint_location,
            stored.previous_location,
            stored.output,
        )

    def put(self, result: CommandResult) -> None:
        if result.state == CommandState.RUNNING:
            raise JournalError('running command can not be journaled')

        self._entries[result.request.command_id] = _Stored(
            _fingerprint(result.request),
            result.state,
            result.value,
            result.checkpoint_location,
            result.previous_location,
            result.output,
        )
        self._entries.move_to_end(result.request.command_id)
        while len(self._entries) > MAX_JOURNAL_ENTRIES:
            self._entries.popitem(last=False)

        self._write()

    def _load(self) -> None:
        directory_fd = _directory(self._directory)
        try:
            try:
                journal_fd = os.open(JOURNAL_FILE, READ_FLAGS, dir_fd=directory_fd)
            except FileNotFoundError:
                return
            except OSError as exc:
                raise JournalError('journal file is unsafe') from exc

            try:
                file_stat = os.fstat(journal_fd)
                if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_uid != os.geteuid() \
                        or file_stat.st_mode & UNSAFE_FILE_BITS or file_stat.st_size > MAX_JOURNAL_BYTES:
                    raise JournalError('journal file is invalid')
                with os.fdopen(journal_fd, 'r') as journal_file:
                    journal_fd = -1
                    document = json.load(journal_file)
            except (OSError, TypeError, ValueError) as exc:
                raise JournalError('journal is corrupt') from exc
            finally:
                if journal_fd >= 0:
                    os.close(journal_fd)
        finally:
            os.close(directory_fd)

        self._decode(document)

    def _decode(self, document: object) -> None:
        if not isinstance(document, dict):
            raise JournalError('journal document is invalid')
        data = cast(Dict[str, object], document)
        if data.get('version') != JOURNAL_VERSION or not isinstance(data.get('entries'), list):
            raise JournalError('journal version is incompatible')
        entries = cast(List[object], data['entries'])
        if len(entries) > MAX_JOURNAL_ENTRIES:
            raise JournalError('journal has too many entries')

        decoded: 'OrderedDict[str, _Stored]' = OrderedDict()
        for raw_entry in entries:
            command_id, stored = _entry(raw_entry)
            if command_id in decoded:
                raise JournalError('journal has duplicate command IDs')
            decoded[command_id] = stored
        self._entries = decoded

    def _write(self) -> None:
        document = {
            'version': JOURNAL_VERSION,
            'entries': [
                {
                    'command_id': command_id,
                    'fingerprint': stored.fingerprint,
                    'state': stored.state.value,
                    'value': stored.value.value,
                    'checkpoint_location': stored.checkpoint_location,
                    'previous_location': stored.previous_location,
                    'output': list(stored.output),
                }
                for command_id, stored in self._entries.items()
            ],
        }
        payload = json.dumps(document, separators=(',', ':'), sort_keys=True).encode('utf-8')
        if len(payload) > MAX_JOURNAL_BYTES:
            raise JournalError('journal exceeds size limit')

        directory_fd = _directory(self._directory)
        temporary = '.commands-{0}.tmp'.format(uuid4())
        temporary_fd = -1
        try:
            temporary_fd = os.open(temporary, WRITE_FLAGS, FILE_MODE, dir_fd=directory_fd)
            with os.fdopen(temporary_fd, 'wb') as temporary_file:
                temporary_fd = -1
                temporary_file.write(payload)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary, JOURNAL_FILE, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
            os.fsync(directory_fd)
        except OSError as exc:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except OSError:
                pass
            raise JournalError('journal write failed') from exc
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
            os.close(directory_fd)


def _directory(path: str) -> int:
    try:
        directory_fd = os.open(path, DIRECTORY_FLAGS)
        directory_stat = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_uid != os.geteuid() \
                or directory_stat.st_mode & UNSAFE_DIRECTORY_BITS:
            os.close(directory_fd)
            raise JournalError('journal root is not a directory')
        return directory_fd
    except OSError as exc:
        raise JournalError('journal root is unsafe') from exc


def _entry(value: object) -> Tuple[str, _Stored]:
    if not isinstance(value, dict):
        raise JournalError('journal entry is invalid')
    data = cast(Dict[str, Any], value)
    command_id = data.get('command_id')
    fingerprint = data.get('fingerprint')
    try:
        valid_id = isinstance(command_id, str) and command_id == str(UUID(command_id))
        valid_fingerprint = isinstance(fingerprint, str) and len(fingerprint) == FINGERPRINT_LENGTH \
            and all(character in '0123456789abcdef' for character in fingerprint)
        state = CommandState(data.get('state'))
        result_value = CommandValue(data.get('value'))
        checkpoint_location = _location(data.get('checkpoint_location'))
        previous_location = _location(data.get('previous_location'))
        output = _output(data.get('output', []))
    except (TypeError, ValueError) as exc:
        raise JournalError('journal entry is invalid') from exc
    if not valid_id or not valid_fingerprint or state == CommandState.RUNNING:
        raise JournalError('journal entry is invalid')

    command_id_value = cast(str, command_id)
    fingerprint_value = cast(str, fingerprint)
    return command_id_value, _Stored(
        fingerprint_value,
        state,
        result_value,
        checkpoint_location,
        previous_location,
        output,
    )


def _location(value: object) -> Optional[int]:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise JournalError('journal WAL location is invalid')
    return value


def _output(value: object) -> Tuple[str, ...]:
    if not isinstance(value, list):
        raise JournalError('journal output is invalid')
    items = cast(List[object], value)
    if len(items) > MAX_JOURNAL_ENTRIES:
        raise JournalError('journal output is invalid')
    output: List[str] = []
    for item in items:
        if not isinstance(item, str) or not item or len(item) > FINGERPRINT_LENGTH or '\x00' in item:
            raise JournalError('journal output is invalid')
        output.append(item)
    return tuple(output)


def _fingerprint(command: LifecycleCommand) -> str:
    target = _target(command.follow_target)
    document = [
        command.kind.value,
        command.target_role.value,
        command.timeout,
        command.stop_mode.value,
        command.checkpoint.value,
        [event.value for event in command.events],
        target,
        command.reload.value,
        _recovery_target(command.recovery_target),
        command.clone_mode.value,
        command.divergence.value,
        command.callback.value if command.callback else None,
        command.bootstrap_state.value,
        _sync_plan(command.sync_plan),
        _slot_plan(command.slot_plan),
    ]
    payload = json.dumps(document, separators=(',', ':'), ensure_ascii=True).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def _target(target: Optional[FollowTarget]) -> object:
    if target is None:
        return None
    return [
        target.kind.value,
        target.name,
        target.host,
        target.port,
        target.database,
        target.slot_name,
        target.slot_mode.value,
    ]


def _recovery_target(target: Optional[RecoveryTarget]) -> object:
    if target is None:
        return None
    return [
        target.kind.value,
        target.name,
        target.host,
        target.port,
        target.database,
        target.slot_name,
        target.slot_mode.value,
        target.role,
        target.checkpoint_after_promote,
    ]


def _sync_plan(plan: Optional[SyncPlan]) -> object:
    if plan is None:
        return None
    return [plan.action.value, list(plan.members), plan.count_mode.value, plan.numsync]


def _slot_plan(plan: Optional[SlotPlan]) -> object:
    if plan is None:
        return None
    context = plan.context
    members = [
        [member.name, member.host, member.port, member.database, member.running, member.lsn,
         list(member.tags)]
        for member in context.members
    ]
    slots = [list(slot) for slot in plan.slots]
    return [
        plan.action.value,
        [context.local_name, context.config_present, context.leader, members, list(context.status_slots),
         list(context.retain_slots), list(context.local_tags)],
        slots,
        list(plan.copy_slots),
    ]
