import asyncio

import pytest
import pykube

from k8s_snapshots import errors
from k8s_snapshots.core import volume_from_resource
from tests.fixtures import make_resource
from tests.fixtures.kube import mock_kube

PV_RESOURCE = make_resource(
    pykube.objects.PersistentVolume,
    'test-pv',
)


@pytest.mark.parametrize(
    [
        'resource',  # resource to get volume from
        'resources',  # resources in mocked kube
        'expected_volume_index',  # index in 'resources' for the expected volume
    ],
    [
        pytest.param(
            PV_RESOURCE,
            [PV_RESOURCE],
            0,
            id='valid_from_volume'
        ),
        pytest.param(
            make_resource(
                pykube.objects.PersistentVolumeClaim,
                'test-pvc',
                spec={
                    'volumeName': 'correct-pv'
                }
            ),
            [
                make_resource(
                    pykube.objects.PersistentVolume,
                    'incorrect-pv',
                ),
                make_resource(
                    pykube.objects.PersistentVolume,
                    'correct-pv',
                ),
            ],
            1,
            id='valid_from_volume_claim'
        ),
        pytest.param(
            make_resource(
                pykube.objects.PersistentVolumeClaim,
                'test-pvc',
                spec={
                    'volumeName': 'nonexistent-pv'
                }
            ),
            [
                make_resource(
                    pykube.objects.PersistentVolume,
                    'existing-but-different-pv'
                )
            ],
            None,
            id='no_volume_for_claim',
            marks=pytest.mark.xfail(
                reason='Volume referred by claim\'s .spec.volumeName does not '
                       'exist',
                raises=errors.VolumeNotFound,
                strict=True,
            )
        ),
        pytest.param(
            make_resource(
                pykube.objects.PersistentVolumeClaim,
                'claim-without-spec-volumename',
            ),
            [],
            None,
            id='claim_without_spec_volumeName',
            marks=pytest.mark.xfail(
                reason='Invalid claim spec, missing .spec.volumeName',
                raises=errors.VolumeNotFound,
                strict=True,
            )
        ),
        pytest.param(
            make_resource(
                pykube.objects.Deployment,
                'a-deployment-is-not-a-volume',
                spec={
                    'volumeName': 'test-pv'
                }
            ),
            [
                make_resource(
                    pykube.objects.PersistentVolume,
                    'test-pv'
                )
            ],
            None,
            id='invalid_resource_can_not_get_volume',
            marks=pytest.mark.xfail(
                reason='Invalid resource type',
                raises=errors.VolumeNotFound,
                strict=True,
            )
        )
    ]
)
def test_volume_from_resource(
        fx_context,
        resource,
        resources,
        expected_volume_index,
):
    loop = asyncio.get_event_loop()

    with mock_kube(resources):
        result = loop.run_until_complete(
            volume_from_resource(
                ctx=fx_context,
                resource=resource,
            )
        )

        if expected_volume_index is not None:
            assert result == resources[expected_volume_index]
