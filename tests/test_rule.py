from unittest import mock

import pytest
from unittest.mock import MagicMock
from k8s_snapshots.rule import rule_from_pv, Rule, parse_deltas
from k8s_snapshots.errors import UnsupportedVolume


@pytest.fixture
def fx_deltas():
    return parse_deltas('PT1M P1D')


def make_pv(
        name='fake-pv',
        deltas_annotation='PT1M P1D',
        claim_name='fake-pvc'
) -> MagicMock:

    if deltas_annotation is not None:
        annotations = {

        }

    return MagicMock(
        obj={
            'metadata': {
                'name': name,
            },
            'spec': {
                'claimRef': {
                    'name': claim_name
                }
            }
        }
    )


def test_fails_if_no_zone(fx_deltas):
    v = MagicMock(annotations={
        'pv.kubernetes.io/provisioned-by': 'kubernetes.io/gce-pd'
    }, labels={})
    with pytest.raises(UnsupportedVolume):
        Rule.from_volume(volume=v, source=v, deltas=fx_deltas)


@pytest.mark.skip
def test_rule_from_volume():
    pv = MagicMock(
        metadata=[]
    )
    pass

