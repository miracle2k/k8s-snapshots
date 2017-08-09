from unittest import mock

import pytest

from k8s_snapshots import errors
from k8s_snapshots.context import Context
from tests.fixtures.kube import make_resource, KUBE_CONFIG


@pytest.fixture
def fx_context(request):
    request.getfixturevalue('fx_mock_context_kube_config')
    request.getfixturevalue('fx_mock_context_kube_client')
    ctx = Context({
        'deltas_annotation_key': 'test.k8s-snapshots.example/deltas'
    })
    return ctx


@pytest.fixture
def fx_deltas(request):
    return 'PT10S PT40S'
