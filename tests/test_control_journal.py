import json
import os
import tempfile
import unittest

from pathlib import Path
from uuid import uuid4

from patroni.control import AgentCommands, BootstrapState, CheckpointMode, CloneMode, \
    CommandDriver, CommandKind, CommandResult, CommandState, CommandValue, DesiredRole, \
    DivergencePolicy, DriverResult, LifecycleCommand, ReloadMode, StopMode, SubmitState
from patroni.control.journal import CommandJournal, JournalError


class ImmediateDriver(CommandDriver):

    def __init__(self) -> None:
        self.calls = 0

    def run(self, command, events, cancelled):
        self.calls += 1
        return DriverResult(CommandValue.TRUE, None, None)

    def cancel(self) -> None:
        pass


def command(command_id=None):
    return LifecycleCommand(
        command_id or str(uuid4()), CommandKind.STOP, DesiredRole.UNCHANGED, 10,
        StopMode.FAST, CheckpointMode.DISABLED, (), None, ReloadMode.RESTART,
        None, CloneMode.CONFIGURED, DivergencePolicy.NONE, None, BootstrapState.IDLE,
    )


class TestCommandJournal(unittest.TestCase):

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_terminal_result_survives_restart(self) -> None:
        request = command()
        result = CommandResult(request, CommandState.SUCCEEDED, CommandValue.TRUE, 20, 10)
        journal = CommandJournal(str(self.directory))
        journal.put(result)

        replay = CommandJournal(str(self.directory)).get(request)

        self.assertEqual(result, replay)
        mode = os.stat(self.directory / 'commands.json').st_mode & 0o777
        self.assertEqual(0o600, mode)

    def test_conflicting_reuse_is_reported(self) -> None:
        request = command()
        journal = CommandJournal(str(self.directory))
        journal.put(CommandResult(request, CommandState.FAILED, CommandValue.FALSE, None, None))
        conflict = request._replace(kind=CommandKind.START)

        with self.assertRaises(JournalError):
            journal.get(conflict)

    def test_journal_contains_no_command_payload(self) -> None:
        request = command()
        CommandJournal(str(self.directory)).put(
            CommandResult(request, CommandState.SUCCEEDED, CommandValue.TRUE, None, None),
        )

        text = (self.directory / 'commands.json').read_text()

        self.assertNotIn('fast', text)
        self.assertNotIn('timeout', text)
        self.assertNotIn('authority', text)

    def test_corruption_fails_closed(self) -> None:
        (self.directory / 'commands.json').write_text('{')
        os.chmod(self.directory / 'commands.json', 0o600)

        with self.assertRaises(JournalError):
            CommandJournal(str(self.directory))

    def test_incompatible_version_fails_closed(self) -> None:
        (self.directory / 'commands.json').write_text(json.dumps({'version': 2, 'entries': []}))
        os.chmod(self.directory / 'commands.json', 0o600)

        with self.assertRaises(JournalError):
            CommandJournal(str(self.directory))

    def test_symlink_is_rejected(self) -> None:
        target = self.directory / 'target'
        target.write_text('{}')
        (self.directory / 'commands.json').symlink_to(target)

        with self.assertRaises(JournalError):
            CommandJournal(str(self.directory))

    def test_writable_directory_is_rejected(self) -> None:
        os.chmod(self.directory, 0o777)

        with self.assertRaises(JournalError):
            CommandJournal(str(self.directory))

        os.chmod(self.directory, 0o700)

    def test_agent_restart_replays_without_execution(self) -> None:
        request = command()
        first_driver = ImmediateDriver()
        first = AgentCommands(first_driver, CommandJournal(str(self.directory)))
        first.submit(request)
        result = first.wait(request.command_id, 1)
        first.close()

        second_driver = ImmediateDriver()
        second = AgentCommands(second_driver, CommandJournal(str(self.directory)))
        replay = second.submit(request)
        second.close()

        self.assertEqual(CommandState.SUCCEEDED, result.state)
        self.assertEqual(SubmitState.REPLAYED, replay.state)
        self.assertEqual(0, second_driver.calls)


if __name__ == '__main__':
    unittest.main()
