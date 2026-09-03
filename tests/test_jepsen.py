from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
COMPOSE = ROOT / 'jepsen' / 'docker-compose.yml'
ROLLOUT = ROOT / 'jepsen' / 'jepsen' / 'rollout.sh'


def test_controller_hosts_render_as_yaml() -> None:
    compose = yaml.safe_load(COMPOSE.read_text())
    hosts = compose['services']['patroni1']['environment']['PATRONI_ETCD3_HOSTS']

    config = yaml.safe_load("etcd3:\n  hosts: '{0}'\n".format(hosts))

    assert config['etcd3']['hosts'] == hosts


def test_rollout_selects_split_replica() -> None:
    script = ROLLOUT.read_text()

    assert 'test -d /etc/service/patroni-agent' in script
