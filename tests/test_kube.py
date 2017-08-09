import pykube

from k8s_snapshots import kube
from tests.fixtures.kube import mock_kube


def test_mock_kube(fx_context):
    n_resources = 5
    volume_names = [f'test-volume-{i}' for i in range(0, n_resources)]

    def _volume(name, namespace='default'):
        return pykube.objects.PersistentVolume(
            fx_context.kube_client(),
            {
                'apiVersion': 'v1',
                'kind': 'PersistentVolume',
                'metadata': {
                    'name': name,
                },
            }
        )

    resources = [_volume(volume_name) for volume_name in volume_names]

    with mock_kube(resources) as _kube:
        for expected_resource, volume_name in zip(resources, volume_names):
            assert expected_resource.name == volume_name, \
                'Resources was not ceated properly'
            kube_resource = kube.get_resource_or_none_sync(
                fx_context.kube_client(),
                pykube.objects.PersistentVolume,
                name=volume_name
            )
            assert kube_resource == expected_resource

            assert len(kube_resource.name)
