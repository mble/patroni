import unittest

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
IMAGE_DIR = ROOT / 'kubernetes' / 'controller-agent'
ACCEPTANCE_DIR = ROOT / 'kubernetes' / 'controller-agent-acceptance'
CONTROLLER_UID = 65532
AGENT_UID = 999
CONTROLLER_GID = CONTROLLER_UID
AGENT_GID = AGENT_UID
CONTROL_MODE = '0770'
SOCKET_WORKERS = 2
POSTGRES_PORT = 5432
REST_PORT = 8008


def _read(name: str) -> str:
    return (IMAGE_DIR / name).read_text()


class TestRoleImages(unittest.TestCase):

    def test_images_are_separate(self) -> None:
        controller = _read('Dockerfile.controller')
        agent = _read('Dockerfile.agent')

        self.assertIn('gcr.io/distroless/python3-debian12', controller)
        self.assertIn('patroni.controller', controller)
        self.assertNotIn('postgres:', controller)
        self.assertIn('postgres:', agent)
        self.assertIn('patroni.agent', agent)
        self.assertIn('/usr/bin/dumb-init', agent)

    def test_dependencies_are_bounded(self) -> None:
        controller = _read('requirements-controller.in')
        agent = _read('requirements-agent.in')

        self.assertIn('python-etcd', controller)
        self.assertNotIn('psycopg', controller)
        self.assertIn('psutil', controller)
        self.assertIn('psycopg', agent)
        self.assertIn('psutil', agent)
        self.assertNotIn('python-etcd', agent)

        for role in ('controller', 'agent'):
            dockerfile = _read('Dockerfile.{0}'.format(role))
            lock = _read('requirements-{0}.lock'.format(role))

            self.assertIn('@sha256:', dockerfile)
            self.assertIn('--require-hashes', dockerfile)
            self.assertIn('--hash=sha256:', lock)

    def test_manifest_is_hardened(self) -> None:
        documents = tuple(yaml.safe_load_all(_read('statefulset.yaml')))
        stateful_set = next(document for document in documents if document.get('kind') == 'StatefulSet')
        template = stateful_set['spec']['template']['spec']
        containers = {container['name']: container for container in template['containers']}
        init_containers = {container['name']: container for container in template['initContainers']}

        self.assertFalse(template['automountServiceAccountToken'])
        self.assertEqual(AGENT_GID, template['securityContext']['fsGroup'])
        self.assertEqual('RuntimeDefault', template['securityContext']['seccompProfile']['type'])
        self.assertEqual(AGENT_UID, containers['agent']['securityContext']['runAsUser'])
        self.assertEqual(AGENT_GID, containers['agent']['securityContext']['runAsGroup'])
        self.assertEqual(CONTROLLER_UID, containers['controller']['securityContext']['runAsUser'])
        self.assertEqual(CONTROLLER_GID, containers['controller']['securityContext']['runAsGroup'])
        self.assertNotEqual(containers['agent']['image'], containers['controller']['image'])

        control_init = init_containers['prepare-control']
        self.assertEqual(containers['agent']['image'], control_init['image'])
        self.assertEqual(0, control_init['securityContext']['runAsUser'])
        self.assertIn('chmod {0} /run/patroni'.format(CONTROL_MODE), control_init['args'][0])
        self.assertEqual({'CHOWN'}, set(control_init['securityContext']['capabilities']['add']))

        ports = {
            name: {port['containerPort'] for port in container['ports']}
            for name, container in containers.items()
        }
        self.assertEqual({POSTGRES_PORT}, ports['agent'])
        self.assertEqual({REST_PORT}, ports['controller'])

        mounts = {
            name: {mount['name'] for mount in container['volumeMounts']}
            for name, container in containers.items()
        }
        self.assertIn('pgdata', mounts['agent'])
        self.assertNotIn('pgdata', mounts['controller'])
        self.assertIn('etcd-tls', mounts['controller'])
        self.assertNotIn('etcd-tls', mounts['agent'])
        self.assertIn('control', mounts['agent'] & mounts['controller'])

        for container in containers.values():
            security = container['securityContext']

            self.assertFalse(security['allowPrivilegeEscalation'])
            self.assertTrue(security['readOnlyRootFilesystem'])
            self.assertEqual(['ALL'], security['capabilities']['drop'])

    def test_socket_binds_roles(self) -> None:
        agent_config = yaml.safe_load(_read('agent.yml'))
        agent = agent_config['agent']
        controller = yaml.safe_load(_read('controller.yml'))['controller']

        self.assertEqual('/var/lib/postgresql/data/pgdata', agent_config['postgresql']['data_dir'])
        self.assertEqual('/tmp', agent_config['postgresql']['parameters']['unix_socket_directories'])
        self.assertEqual(SOCKET_WORKERS, agent['max_workers'])
        self.assertEqual(CONTROLLER_UID, agent['peer_uid'])
        self.assertEqual(CONTROLLER_GID, agent['peer_gid'])
        self.assertEqual(AGENT_UID, controller['peer_uid'])
        self.assertEqual(AGENT_GID, controller['peer_gid'])

    def test_acceptance_can_stop_controller(self) -> None:
        patch = yaml.safe_load((ACCEPTANCE_DIR / 'statefulset-patch.yaml').read_text())
        controller = patch['spec']['template']['spec']['containers'][0]

        self.assertEqual(['/usr/bin/python3', '-c'], controller['command'])
        self.assertIn('subprocess.call', controller['args'][0])

    def test_acceptance_etcd_is_pinned(self) -> None:
        documents = tuple(yaml.safe_load_all((ACCEPTANCE_DIR / 'etcd.yaml').read_text()))
        deployment = next(document for document in documents if document.get('kind') == 'Deployment')
        image = deployment['spec']['template']['spec']['containers'][0]['image']

        self.assertIn('@sha256:', image)
