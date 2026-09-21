import unittest

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github' / 'workflows' / 'jepsen.yaml'
RUN_STEP = 'Run test'
JEPSEN_ENV_NAMES = (
    'JEPSEN_SEED',
    'JEPSEN_TIME_LIMIT',
    'JEPSEN_FINAL_TIME_LIMIT',
    'JEPSEN_RECOVERY_SECONDS',
    'JEPSEN_RUN_TIMEOUT',
)


class TestJepsenWorkflow(unittest.TestCase):

    def test_run_passes_limits_to_container(self) -> None:
        workflow = yaml.safe_load(WORKFLOW.read_text())
        step = next(step for step in workflow['jobs']['jepsen']['steps'] if step['name'] == RUN_STEP)
        command = step['run']

        self.assertIn('set -o pipefail', command)
        for name in JEPSEN_ENV_NAMES:
            self.assertIn('-e {0}="${{{0}}}"'.format(name), command)
