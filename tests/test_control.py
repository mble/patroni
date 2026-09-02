import unittest

from uuid import UUID, uuid4

from patroni.control import AgentState, AuthorityGrant, AuthorityKind, AuthorityState, \
    CommandKind, CommandPhase, CommandRequest, CommandState, DesiredRole, PolicyMode, \
    PostgresRole, SafetyAction, SafetyState, Timing, ValidationError

HISTORY_LIMIT = 2
TTL = 30.0
LOOP_WAIT = 10.0
RETRY_TIMEOUT = 10.0
WATCHDOG_TIMEOUT = 25.0


class FakeClock:

    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakePostgres:

    def __init__(self) -> None:
        self.role = PostgresRole.REPLICA
        self.actions = []

    def apply(self, action: SafetyAction) -> None:
        self.actions.append(action)
        if action == SafetyAction.FENCE:
            self.role = PostgresRole.REPLICA


class TestSafetyState(unittest.TestCase):

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.controller_id = str(uuid4())
        self.agent_id = str(uuid4())
        self.state = SafetyState(self.agent_id, self.controller_id, self.clock, HISTORY_LIMIT)
        self.timing = Timing(TTL, LOOP_WAIT, RETRY_TIMEOUT, WATCHDOG_TIMEOUT)

    def grant(self, kind: AuthorityKind = AuthorityKind.LEADER, term: int = 1,
              sequence: int = 1, lifetime: float = WATCHDOG_TIMEOUT) -> AuthorityGrant:
        grant = AuthorityGrant(kind, self.controller_id, self.agent_id, term, sequence,
                               self.clock(), self.clock() + lifetime, self.timing)
        self.state.grant(grant)
        return grant

    def command(self, kind: CommandKind = CommandKind.PROMOTE, sequence: int = 2,
                target: DesiredRole = DesiredRole.PRIMARY, term: int = 1,
                command_id: str = '') -> CommandRequest:
        return CommandRequest(command_id or str(uuid4()), self.controller_id, self.agent_id,
                              sequence, kind, target, term)

    def test_grant_validates_identity_sequence_and_deadline(self) -> None:
        self.grant()
        snapshot = self.state.snapshot

        bad = [
            AuthorityGrant(AuthorityKind.LEADER, 'bad', self.agent_id, 2, 2,
                           self.clock(), self.clock() + 1, self.timing),
            AuthorityGrant(AuthorityKind.LEADER, self.controller_id, str(uuid4()), 2, 2,
                           self.clock(), self.clock() + 1, self.timing),
            AuthorityGrant(AuthorityKind.LEADER, self.controller_id, self.agent_id, 0, 2,
                           self.clock(), self.clock() + 1, self.timing),
            AuthorityGrant(AuthorityKind.LEADER, self.controller_id, self.agent_id, 2, 1,
                           self.clock(), self.clock() + 1, self.timing),
            AuthorityGrant(AuthorityKind.LEADER, self.controller_id, self.agent_id, 2, 2,
                           self.clock(), self.clock(), self.timing),
            AuthorityGrant(AuthorityKind.LEADER, self.controller_id, self.agent_id, 2, 2,
                           self.clock(), self.clock() + TTL + 1, self.timing),
        ]

        for grant in bad:
            with self.subTest(grant=grant):
                with self.assertRaises(ValidationError):
                    self.state.grant(grant)
                self.assertEqual(snapshot, self.state.snapshot)

    def test_timing_validation(self) -> None:
        invalid = [
            Timing(0, LOOP_WAIT, RETRY_TIMEOUT, WATCHDOG_TIMEOUT),
            Timing(TTL, LOOP_WAIT, RETRY_TIMEOUT + 1, WATCHDOG_TIMEOUT),
            Timing(TTL, LOOP_WAIT, RETRY_TIMEOUT, TTL + 1),
        ]

        for timing in invalid:
            with self.subTest(timing=timing):
                with self.assertRaises(ValidationError):
                    self.state.grant(AuthorityGrant(
                        AuthorityKind.LEADER, self.controller_id, self.agent_id, 1, 1,
                        self.clock(), self.clock() + 1, timing))

    def test_promote_requires_current_leader_authority(self) -> None:
        request = self.command(sequence=1)
        self.assertEqual(SafetyAction.REJECT, self.state.submit(request).action)

        self.grant(sequence=2)
        self.assertEqual(SafetyAction.RUN, self.state.submit(self.command(sequence=3)).action)

    def test_commands_fail_closed_without_authority(self) -> None:
        cases = {
            CommandKind.START: (DesiredRole.PRIMARY, SafetyAction.REJECT),
            CommandKind.STOP: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.RESTART: (DesiredRole.PRIMARY, SafetyAction.REJECT),
            CommandKind.PROMOTE: (DesiredRole.PRIMARY, SafetyAction.REJECT),
            CommandKind.FOLLOW: (DesiredRole.REPLICA, SafetyAction.RUN),
            CommandKind.BOOTSTRAP: (DesiredRole.PRIMARY, SafetyAction.REJECT),
            CommandKind.CLONE: (DesiredRole.REPLICA, SafetyAction.RUN),
            CommandKind.REWIND: (DesiredRole.REPLICA, SafetyAction.RUN),
            CommandKind.CRASH_RECOVERY: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.POST_BOOTSTRAP: (DesiredRole.PRIMARY, SafetyAction.REJECT),
            CommandKind.REINITIALIZE: (DesiredRole.REPLICA, SafetyAction.RUN),
            CommandKind.APPLY_CONFIG: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.CALLBACK: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.REMOVE_DATA: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.MOVE_DATA: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.SET_BOOTSTRAP: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.RESET_RECOVERY: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.CHECK_DIVERGENCE: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.ARCHIVE_WAL: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.APPLY_SYNC: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.APPLY_SLOTS: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.COPY_SLOTS: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.CHECKPOINT: (DesiredRole.UNCHANGED, SafetyAction.RUN),
            CommandKind.FENCE: (DesiredRole.UNCHANGED, SafetyAction.FENCE),
        }
        self.assertEqual(set(CommandKind), set(cases))

        for kind, (target, expected) in cases.items():
            with self.subTest(kind=kind):
                state = SafetyState(self.agent_id, self.controller_id,
                                    self.clock, HISTORY_LIMIT)
                request = self.command(kind, sequence=1, target=target)
                self.assertEqual(expected, state.submit(request).action)

    def test_bootstrap_requires_initializer_authority(self) -> None:
        self.grant(AuthorityKind.LEADER)
        request = self.command(CommandKind.BOOTSTRAP, target=DesiredRole.PRIMARY)
        self.assertEqual(SafetyAction.REJECT, self.state.submit(request).action)

        state = SafetyState(self.agent_id, self.controller_id, self.clock, HISTORY_LIMIT)
        self.state = state
        self.grant(AuthorityKind.INITIALIZER)
        self.assertEqual(SafetyAction.RUN, self.state.submit(request).action)

    def test_failsafe_preserves_but_does_not_create_primary(self) -> None:
        self.grant(AuthorityKind.FAILSAFE)
        request = self.command()
        self.assertEqual(SafetyAction.REJECT, self.state.submit(request).action)

        self.state.observe(PostgresRole.PRIMARY)
        request = self.command(CommandKind.APPLY_CONFIG, sequence=3,
                               target=DesiredRole.UNCHANGED)
        self.assertEqual(SafetyAction.RUN, self.state.submit(request).action)

    def test_failsafe_cannot_restart_primary(self) -> None:
        self.grant(AuthorityKind.FAILSAFE)
        self.state.observe(PostgresRole.PRIMARY)

        request = self.command(CommandKind.RESTART)
        self.assertEqual(SafetyAction.REJECT, self.state.submit(request).action)

    def test_authority_expires_at_deadline(self) -> None:
        self.grant(lifetime=1)
        self.assertEqual(AuthorityState.CURRENT, self.state.snapshot.authority_state)
        self.clock.advance(1)

        self.assertEqual(AuthorityState.EXPIRED, self.state.snapshot.authority_state)
        self.assertEqual(SafetyAction.REJECT, self.state.submit(self.command()).action)

    def test_primary_without_authority_fences_when_active(self) -> None:
        action = self.state.observe(PostgresRole.PRIMARY)

        self.assertEqual(SafetyAction.FENCE, action)
        self.assertEqual(AgentState.FENCING, self.state.snapshot.agent_state)

    def test_primary_without_authority_survives_when_paused(self) -> None:
        self.state.policy(PolicyMode.PAUSED, 1)

        self.assertEqual(SafetyAction.NONE, self.state.observe(PostgresRole.PRIMARY))
        self.clock.advance(TTL)
        self.assertEqual(SafetyAction.NONE, self.state.tick())

    def test_resume_without_authority_fences_primary(self) -> None:
        self.state.policy(PolicyMode.PAUSED, 1)
        self.state.observe(PostgresRole.PRIMARY)

        self.assertEqual(SafetyAction.FENCE, self.state.policy(PolicyMode.ACTIVE, 2))

    def test_disconnect_waits_for_authority_expiry(self) -> None:
        self.grant(lifetime=1)
        self.state.observe(PostgresRole.PRIMARY)

        self.state.disconnect()
        self.assertEqual(SafetyAction.NONE, self.state.tick())
        self.clock.advance(1)
        self.assertEqual(SafetyAction.FENCE, self.state.tick())

    def test_disconnect_while_paused_does_not_fence(self) -> None:
        self.state.policy(PolicyMode.PAUSED, 1)
        self.state.observe(PostgresRole.PRIMARY)
        self.state.disconnect()
        self.clock.advance(TTL)

        self.assertEqual(SafetyAction.NONE, self.state.tick())

    def test_explicit_fence_preempts_every_phase(self) -> None:
        for phase in CommandPhase:
            with self.subTest(phase=phase):
                state = SafetyState(self.agent_id, self.controller_id, self.clock, HISTORY_LIMIT)
                self.state = state
                self.grant()
                request = self.command()
                state.submit(request)
                state.advance(request.command_id, phase)

                self.assertEqual(SafetyAction.FENCE, state.fence())
                self.assertEqual(CommandState.FENCED, state.command(request.command_id).state)

    def test_fence_command_preempts_active_work(self) -> None:
        self.grant()
        request = self.command()
        self.state.submit(request)
        fence = self.command(CommandKind.FENCE, sequence=3,
                             target=DesiredRole.UNCHANGED)

        receipt = self.state.submit(fence)

        self.assertEqual(SafetyAction.FENCE, receipt.action)
        self.assertEqual(CommandState.FENCED, receipt.state)
        self.assertEqual(CommandState.FENCED, self.state.command(request.command_id).state)
        self.assertEqual(AgentState.FENCING, self.state.snapshot.agent_state)

    def test_expiry_preempts_promotion(self) -> None:
        self.grant(lifetime=1)
        request = self.command()
        self.state.submit(request)
        self.clock.advance(1)

        self.assertEqual(SafetyAction.FENCE,
                         self.state.advance(request.command_id, CommandPhase.MUTATING))

    def test_expiry_at_promotion_completion_fences(self) -> None:
        self.grant(lifetime=1)
        request = self.command()
        self.state.submit(request)
        self.state.advance(request.command_id, CommandPhase.MUTATING)
        self.clock.advance(1)

        self.assertEqual(SafetyAction.FENCE,
                         self.state.complete(request.command_id, CommandState.SUCCEEDED))
        self.assertEqual(CommandState.FENCED, self.state.command(request.command_id).state)

    def test_new_authority_term_preempts_old_promotion(self) -> None:
        self.grant()
        request = self.command()
        self.state.submit(request)
        self.state.grant(AuthorityGrant(
            AuthorityKind.LEADER, self.controller_id, self.agent_id, 2, 3,
            self.clock(), self.clock() + WATCHDOG_TIMEOUT, self.timing))

        self.assertEqual(SafetyAction.FENCE,
                         self.state.advance(request.command_id, CommandPhase.MUTATING))

    def test_initializer_expiry_fences_bootstrapped_primary(self) -> None:
        self.grant(AuthorityKind.INITIALIZER, lifetime=1)
        request = self.command(CommandKind.BOOTSTRAP)
        self.state.submit(request)
        self.state.observe(PostgresRole.PRIMARY)
        self.clock.advance(1)

        self.assertEqual(SafetyAction.FENCE, self.state.tick())

    def test_failsafe_expiry_fences_primary(self) -> None:
        self.grant(AuthorityKind.FAILSAFE, lifetime=1)
        self.state.observe(PostgresRole.PRIMARY)
        self.clock.advance(1)

        self.assertEqual(SafetyAction.FENCE, self.state.tick())

    def test_duplicate_returns_original_status(self) -> None:
        self.grant()
        request = self.command()
        first = self.state.submit(request)
        duplicate = request._replace(sequence=request.sequence + 1)

        repeated = self.state.submit(duplicate)
        self.assertEqual(SafetyAction.NONE, repeated.action)
        self.assertEqual(first.state, repeated.state)

        self.state.complete(request.command_id, CommandState.SUCCEEDED)
        self.assertEqual(CommandState.SUCCEEDED, self.state.submit(duplicate).state)

    def test_new_duplicate_sequence_is_consumed(self) -> None:
        self.grant()
        request = self.command()
        self.state.submit(request)
        duplicate = request._replace(sequence=3)
        self.state.submit(duplicate)
        snapshot = self.state.snapshot

        with self.assertRaises(ValidationError):
            self.state.submit(self.command(CommandKind.RESTART, sequence=3))

        self.assertEqual(snapshot, self.state.snapshot)

    def test_conflicting_command_id_is_rejected(self) -> None:
        self.grant()
        request = self.command()
        self.state.submit(request)
        conflict = request._replace(sequence=request.sequence + 1, kind=CommandKind.RESTART)

        with self.assertRaises(ValidationError):
            self.state.submit(conflict)

    def test_stale_command_has_no_side_effect(self) -> None:
        self.grant()
        snapshot = self.state.snapshot

        with self.assertRaises(ValidationError):
            self.state.submit(self.command(sequence=1))

        self.assertEqual(snapshot, self.state.snapshot)

    def test_command_phase_cannot_move_backwards(self) -> None:
        self.grant()
        request = self.command()
        self.state.submit(request)
        self.state.advance(request.command_id, CommandPhase.MUTATING)
        snapshot = self.state.snapshot

        with self.assertRaises(ValidationError):
            self.state.advance(request.command_id, CommandPhase.PREPARING)

        self.assertEqual(snapshot, self.state.snapshot)

    def test_only_terminal_results_complete_commands(self) -> None:
        self.grant()
        request = self.command()
        self.state.submit(request)
        snapshot = self.state.snapshot

        with self.assertRaises(ValidationError):
            self.state.complete(request.command_id, CommandState.RUNNING)

        self.assertEqual(snapshot, self.state.snapshot)

    def test_history_is_bounded(self) -> None:
        self.grant()
        command_ids = []

        for sequence in range(2, 5):
            request = self.command(CommandKind.APPLY_CONFIG, sequence,
                                   DesiredRole.UNCHANGED)
            command_ids.append(request.command_id)
            self.state.submit(request)
            self.state.complete(request.command_id, CommandState.SUCCEEDED)

        with self.assertRaises(KeyError):
            self.state.command(command_ids[0])
        self.assertEqual(CommandState.SUCCEEDED, self.state.command(command_ids[-1]).state)

    def test_fence_completion_requires_non_primary(self) -> None:
        postgres = FakePostgres()
        postgres.role = PostgresRole.PRIMARY
        postgres.apply(self.state.observe(postgres.role))

        self.state.fence_complete(postgres.role)

        self.assertEqual(AgentState.IDLE, self.state.snapshot.agent_state)
        self.assertEqual(PostgresRole.REPLICA, self.state.snapshot.postgres_role)

    def test_fence_completion_requires_fencing_state(self) -> None:
        snapshot = self.state.snapshot

        with self.assertRaises(ValidationError):
            self.state.fence_complete(PostgresRole.REPLICA)

        self.assertEqual(snapshot, self.state.snapshot)

    def test_boot_ids_are_canonical_uuids(self) -> None:
        noncanonical = str(UUID(self.agent_id)).upper()

        with self.assertRaises(ValidationError):
            SafetyState(noncanonical, self.controller_id, self.clock, HISTORY_LIMIT)

    def test_dynamic_timing_shortens_authority(self) -> None:
        self.grant(lifetime=WATCHDOG_TIMEOUT)
        short = Timing(12, 4, 4, 8)
        self.state.grant(AuthorityGrant(
            AuthorityKind.LEADER, self.controller_id, self.agent_id, 1, 2,
            self.clock(), self.clock() + short.watchdog_timeout, short))
        self.state.observe(PostgresRole.PRIMARY)
        self.clock.advance(short.watchdog_timeout)

        self.assertEqual(SafetyAction.FENCE, self.state.tick())


if __name__ == '__main__':
    unittest.main()
