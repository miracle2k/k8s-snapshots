import asyncio
from datetime import timedelta

import pykube

from k8s_snapshots.rule import rule_from_pv
from tests.fixtures import make_resource


def test_rule_from_pv_uses_topology_zone_for_aws_region(
    fx_context,
):
    volume = make_resource(
        pykube.objects.PersistentVolume,
        'pvc-tronbyt-anon',
        spec={
            'csi': {
                'driver': 'ebs.csi.aws.com',
                'volumeHandle': 'vol-0123456789abcdef0',
            },
            'nodeAffinity': {
                'required': {
                    'nodeSelectorTerms': [
                        {
                            'matchExpressions': [
                                {
                                    'key': 'topology.kubernetes.io/zone',
                                    'operator': 'In',
                                    'values': ['eu-west-2a'],
                                },
                            ],
                        },
                    ],
                },
            },
        },
    )

    loop = asyncio.get_event_loop()
    rule = loop.run_until_complete(
        rule_from_pv(
            ctx=fx_context,
            volume=volume,
            deltas=[timedelta(minutes=1), timedelta(minutes=2)],
            source=volume,
        )
    )

    assert rule.backend == 'aws'
    assert rule.disk.volume_id == 'vol-0123456789abcdef0'
    assert rule.disk.region == 'eu-west-2'
