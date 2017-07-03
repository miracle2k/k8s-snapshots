import os
import pytest
import contextlib
import datetime
import k8s_snapshots.config
from k8s_snapshots.logconf import configure_logging
from k8s_snapshots.config import read_volume_config


@pytest.fixture(autouse=True, scope='session')
def setup_logging():
    os.environ['GCLOUD_PROJECT'] = 'foo'
    config = k8s_snapshots.config.from_environ()
    configure_logging(config)


@contextlib.contextmanager
def set_env(**environ):
    """
    Temporarily set the process environment variables.

    >>> with set_env(PLUGINS_DIR=u'test/plugins'):
    ...   "PLUGINS_DIR" in os.environ
    True

    >>> "PLUGINS_DIR" in os.environ
    False

    :type environ: dict[str, unicode]
    :param environ: Environment variables to set
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


class TestManualVolumes:
    """Test finding the manual list of volumes.
    """

    def test_find_volumes(self):
        with set_env(**{
            'VOLUMES': 'foo,bar',
            'VOLUME_FOO_DELTAS': '1d 7d',
            'VOLUME_FOO_ZONE': 'eu-central-1',
            'VOLUME_BAR_DELTAS': '1d 2d',
            'VOLUME_BAR_ZONE': 'eu-west-1',
        }):
            rules = read_volume_config()['rules']
            assert len(rules) == 2
            assert rules[0].name == 'foo'
            assert rules[0].namespace == ''
            assert rules[0].deltas == [datetime.timedelta(1), datetime.timedelta(7)]
            assert rules[0].gce_disk == 'foo'
            assert rules[1].name == 'bar'
            assert rules[1].namespace == ''
            assert rules[1].deltas == [datetime.timedelta(1), datetime.timedelta(2)]
            assert rules[1].gce_disk == 'bar'
            assert rules[1].gce_disk_zone == 'eu-west-1'
            assert rules[1].claim_name == ''
