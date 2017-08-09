import os
import pytest
import contextlib
import datetime
from k8s_snapshots.config import read_volume_config


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
    """
    Test finding the manual list of volumes.
    """

    def test_find_volumes(self):
        with set_env(**{
            'VOLUMES': 'foo,bar',
            'VOLUME_FOO_DELTAS': 'P1D P7D',
            'VOLUME_FOO_ZONE': 'eu-central-1',
            'VOLUME_BAR_DELTAS': 'P1D P2D',
            'VOLUME_BAR_ZONE': 'eu-west-1',
        }):
            rules = read_volume_config()['rules']
            assert len(rules) == 2
            assert rules[0].name == 'foo'
            assert rules[0].source is None
            assert rules[0].deltas == [datetime.timedelta(1), datetime.timedelta(7)]
            assert rules[0].gce_disk == 'foo'
            assert rules[1].name == 'bar'
            assert rules[1].source is None
            assert rules[1].deltas == [datetime.timedelta(1), datetime.timedelta(2)]
            assert rules[1].gce_disk == 'bar'
            assert rules[1].gce_disk_zone == 'eu-west-1'
