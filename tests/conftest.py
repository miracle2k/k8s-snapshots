import pytest

from k8s_snapshots.logconf import configure_logging


@pytest.fixture(scope='session', autouse=True)
def configured_logging():
    configure_logging(
        level_name='DEBUG',
        for_humans=True,
    )

