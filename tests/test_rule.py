import pytest
from unittest.mock import MagicMock
from k8s_snapshots.rule import rule_from_pv
from k8s_snapshots.errors import AnnotationError
from pykube.objects import PersistentVolume


class TestRuleFromPV:
    def test_fails_if_no_zone(self):

        v = MagicMock(annotations={
            'pv.kubernetes.io/provisioned-by': 'kubernetes.io/gce-pd'
        }, labels={})
        with pytest.raises(AnnotationError):
            rule_from_pv(volume=v, api=None, deltas_annotation_key='test')