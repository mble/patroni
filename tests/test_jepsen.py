from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
COMPOSE = ROOT / 'jepsen' / 'docker-compose.yml'


def test_controller_hosts_render_as_yaml() -> None:
    compose = yaml.safe_load(COMPOSE.read_text())
    hosts = compose['services']['patroni1']['environment']['PATRONI_ETCD3_HOSTS']

    config = yaml.safe_load("etcd3:\n  hosts: '{0}'\n".format(hosts))

    assert config['etcd3']['hosts'] == hosts
