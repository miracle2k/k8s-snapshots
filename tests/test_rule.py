import asyncio
from typing import Dict, Optional, Any

import pykube
import pytest

from k8s_snapshots import errors
from k8s_snapshots.context import Context
from k8s_snapshots.rule import rule_from_pv, Rule, parse_deltas
from tests.fixtures.kube import (
    mock_kube,
    make_volume_and_claim,
    make_resource,
    LABEL_ZONE,
    spec_gce_persistent_disk)


@pytest.mark.parametrize(
    ['deltas_annotation_key'],
    [
        pytest.param(
            'test-snapshots.kubernetes.io/deltas',
            id='cfg_annotation_key_0',
        ),
        pytest.param(
            'foo/deltas',
            id='cfg_annotation_key_1',
        )
    ]
)
@pytest.mark.parametrize(
    ['volume_zone_label'],
    [
        pytest.param(
            LABEL_ZONE,
            id='zone_label_present'
        ),
        pytest.param(
            {},
            id='zone_label_absent',
            marks=pytest.mark.xfail(
                reason='Missing zone label',
                raises=errors.UnsupportedVolume,
                strict=True
            )
        )
    ]
)
def test_rule_from_volume_with_claim(
        deltas_annotation_key,
        fx_deltas,
        volume_zone_label
):
    ctx = Context({
        'deltas_annotation_key': deltas_annotation_key
    })

    pv, pvc = make_volume_and_claim(
        ctx=ctx,
        claim_annotations={
            ctx.config['deltas_annotation_key']: fx_deltas,
        },
        volume_zone_label=volume_zone_label,
    )

    if pvc.namespace and not pvc.namespace == 'default':
        expected_rule_name = f'pvc-{pvc.namespace}-{pvc.name}'
    else:
        expected_rule_name = f'pvc-{pvc.name}'

    loop = asyncio.get_event_loop()
    with mock_kube([pv, pvc]) as _mocked:
        deltas_annotation_key = ctx.config['deltas_annotation_key']
        rule = loop.run_until_complete(
            rule_from_pv(
                ctx,
                pv,
                deltas_annotation_key
            )
        )
        assert rule.source == pvc.obj['metadata']['selfLink']
        assert deltas_annotation_key in pvc.annotations
        assert rule.name == expected_rule_name
        assert rule.deltas == parse_deltas(fx_deltas)


@pytest.mark.parametrize(
    [
        'label_zone',
        '_spec_gce_persistent_disk',
    ],
    [
        pytest.param(
            LABEL_ZONE,
            spec_gce_persistent_disk('test-pd'),
            id='valid'
        ),
        pytest.param(
            LABEL_ZONE,
            {},
            id='missing_disk_identifier',
            marks=pytest.mark.xfail(
                reason='Missing disk identifier',
                raises=errors.UnsupportedVolume,
                strict=True
            )
        )
    ]
)
def test_rule_from_volume(
        fx_context: Context,
        fx_deltas: str,
        label_zone: Optional[Dict[str, str]],
        _spec_gce_persistent_disk: Optional[Dict[str, Any]]
):
    annotations = {}

    annotations.update({
        fx_context.config['deltas_annotation_key']: fx_deltas,
    })

    labels = {}

    if label_zone is not None:
        labels.update(label_zone)

    spec = {}

    if _spec_gce_persistent_disk is not None:
        spec.update(_spec_gce_persistent_disk)

    source_pv = make_resource(
        pykube.objects.PersistentVolume,
        'source-pv',
        annotations=annotations,
        labels=labels,
        spec=spec,
    )

    expected_rule_name = f'pv-{source_pv.name}'

    loop = asyncio.get_event_loop()

    with mock_kube([source_pv]):
        rule: Rule = loop.run_until_complete(
            rule_from_pv(
                fx_context,
                source_pv,
                fx_context.config['deltas_annotation_key']
            )
        )
        assert rule.name == expected_rule_name
        assert rule.deltas == parse_deltas(fx_deltas)


@pytest.mark.parametrize(
    ['claim_namespace'],
    [
        pytest.param('default'),
        pytest.param('test-namespace'),
    ]
)
def test_rule_name_from_pvc(fx_context, claim_namespace):
    claim_name = 'source-pvc'

    source_pv = make_resource(
        pykube.objects.PersistentVolume,
        'source-pv',
        labels=LABEL_ZONE,
        spec={
            'claimRef': {
                'name': claim_name,
                'namespace': claim_namespace,
            },
            'gcePersistentDisk': {
                'pdName': 'source-pd',
            }
        }
    )

    source_pvc = make_resource(
        pykube.objects.PersistentVolumeClaim,
        claim_name,
        namespace=claim_namespace,
        annotations={
            fx_context.config['deltas_annotation_key']: 'PT1M PT2M'
        }
    )

    resources = [source_pv, source_pvc]

    if claim_namespace == 'default':
        expected_rule_name = f'pvc-{claim_name}'
    else:
        expected_rule_name = f'{claim_namespace}-pvc-{claim_name}'

    loop = asyncio.get_event_loop()
    with mock_kube(resources):
        async def _run():
            rule = await rule_from_pv(
                ctx=fx_context,
                volume=source_pv,
                deltas_annotation_key=fx_context.config['deltas_annotation_key']
            )

            assert rule.name == expected_rule_name

        loop.run_until_complete(_run())


def test_rule_name_from_pv(
        fx_context,
        fx_volume_zone_label,
        fx_annotation_deltas,
):
    volume_name = 'source-pv'

    annotations = {}
    annotations.update(fx_annotation_deltas)

    source_pv = make_resource(
        pykube.objects.PersistentVolume,
        volume_name,
        annotations=annotations,
        labels=fx_volume_zone_label,
        spec={
            'gcePersistentDisk': {
                'pdName': 'source-pd'
            }
        }
    )

    expected_rule_name = f'pv-{volume_name}'

    loop = asyncio.get_event_loop()
    with mock_kube([source_pv]):
        async def _run():
            rule = await rule_from_pv(
                ctx=fx_context,
                volume=source_pv,
                deltas_annotation_key=fx_context.config['deltas_annotation_key']
            )

            assert rule.name == expected_rule_name

        loop.run_until_complete(_run())

