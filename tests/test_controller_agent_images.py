import unittest

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
IMAGE_DIR = ROOT / 'kubernetes' / 'controller-agent'
CONTROLLER_UID = 65532
AGENT_UID = 999
CONTROLLER_GID = CONTROLLER_UID
AGENT_GID = AGENT_UID
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

        self.assertFalse(template['automountServiceAccountToken'])
        self.assertEqual(AGENT_GID, template['securityContext']['fsGroup'])
        self.assertEqual('RuntimeDefault', template['securityContext']['seccompProfile']['type'])
        self.assertEqual(AGENT_UID, containers['agent']['securityContext']['runAsUser'])
        self.assertEqual(AGENT_GID, containers['agent']['securityContext']['runAsGroup'])
        self.assertEqual(CONTROLLER_UID, containers['controller']['securityContext']['runAsUser'])
        self.assertEqual(CONTROLLER_GID, containers['controller']['securityContext']['runAsGroup'])
        self.assertNotEqual(containers['agent']['image'], containers['controller']['image'])

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

        self.assertEqual('/tmp', agent_config['postgresql']['parameters']['unix_socket_directories'])
        self.assertEqual(SOCKET_WORKERS, agent['max_workers'])
        self.assertEqual(CONTROLLER_UID, agent['peer_uid'])
        self.assertEqual(CONTROLLER_GID, agent['peer_gid'])
        self.assertEqual(AGENT_UID, controller['peer_uid'])
        self.assertEqual(AGENT_GID, controller['peer_gid'])
