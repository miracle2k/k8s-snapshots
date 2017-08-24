import contextlib
from typing import Dict, Iterable, Optional, Tuple, List, Any, Type, Hashable, \
    NamedTuple, Generator, Callable
from unittest import mock
from unittest.mock import MagicMock, Mock

import structlog
import pykube
import pytest
from _pytest.fixtures import FixtureRequest

from k8s_snapshots import kube, errors
from k8s_snapshots.context import Context

_logger = structlog.get_logger(__name__)

KUBE_SAFETY_CHECK_CONFIG_KEY = 'test-fixture-safety-check'

KUBE_CONFIG = pykube.KubeConfig({
    'apiVersion': 'v1',
    'kind': 'Config',
    'clusters': [
        {
            'name': 'test-fixture-cluster',
            'certificate-authority-data': 'From fixture fx_kube_config',
            'server': 'http://test-fixture-server',
        },
    ],
    'contexts': [
        {
            'name': 'test-fixture-context',
            'context': {
                'cluster': 'test-fixture-cluster',
                'user': 'test-fixture-user',
            },
        },
    ],
    'current-context': 'test-fixture-context',
    KUBE_SAFETY_CHECK_CONFIG_KEY: 'I am present',
})

LABEL_ZONE_VALUE = 'test-zone'

LABEL_ZONE_KEY = 'failure-domain.beta.kubernetes.io/zone'
LABEL_ZONE = {LABEL_ZONE_KEY: LABEL_ZONE_VALUE}

DELTAS_ANNOTATION = 'PT1M PT2M'

DEFAULT = object()


@pytest.fixture(scope='session', autouse=True)
def fx_mock_context_kube_config():
    with mock.patch(
            'k8s_snapshots.context.Context.load_kube_config',
            return_value=KUBE_CONFIG) as _mock:
        assert Context().load_kube_config() == KUBE_CONFIG
        yield _mock


@pytest.fixture(scope='session', autouse=True)
def fx_mock_context_kube_client():
    def _fake_client(self: Context):
        return MagicMock(
            spec=pykube.HTTPClient,
            config=self.load_kube_config()
        )
    with mock.patch(
        'k8s_snapshots.context.Context.kube_client',
        _fake_client,
    ) as _mock:
        yield _mock


@pytest.fixture
def fx_kube_config(request: FixtureRequest) -> pykube.KubeConfig:
    """
    Minimal fake pykube.HTTPClient config fixture.
    """
    return KUBE_CONFIG


class MockKubernetes(kube.Kubernetes):
    def __init__(self, *args, **kwargs):
        super(MockKubernetes, self).__init__(*args, **kwargs)

    def get_or_none(
            self,
            resource_type: Type[kube.Resource],
            name: str,
            namespace: Optional[str]=None,
    ) -> Optional[kube.Resource]:
        return self.resource_map.get(
            self.make_key(
                resource_type,
                name,
                namespace
            )
        )

    def watch(
            self,
            resource_type: Type[kube.Resource],
    ):
        raise NotImplementedError

    # Mock-specific methods

    ResourceKey = NamedTuple(
        'ResourceKey',
        [
            ('namespace', str),
            ('resource_type', Type[kube.Resource]),
            ('name', str)
        ]
    )

    resource_map: Dict[ResourceKey, kube.Resource] = {}

    # def filter_resources(
    #         self,
    #         namespace: Optional[str]=None,
    #         resource_type: Optional[Type[kube.Resource]]=None,
    #         name: Optional[str]=None
    # ) -> Generator[kube.Resource, None, None]:
    #     tests: List[Callable[[self.ResourceKey, kube.Resource], bool]]
    #     tests = []
    #     if namespace is not None:
    #         tests.append(lambda k, v: k.namespace == namespace)
    #
    #     if resource_type is not None:
    #         tests.append(lambda k, v: k.resource_type == resource_type)
    #
    #     if name is not None:
    #         tests.append(lambda k, v: k.name == name)
    #
    #     for key, resource in self.resource_map.items():
    #         if all(test(key, resource) for test in tests):
    #             yield resource

    @classmethod
    def resource_key(cls, resource: kube.Resource) -> Hashable:
        return cls.make_key(type(resource), resource.name, resource.namespace)

    @classmethod
    def make_key(
            cls,
            resource_type: Type[kube.Resource],
            name: str,
            namespace: Any=DEFAULT,
    ) -> ResourceKey:
        if namespace is DEFAULT:
            namespace = 'default'
        return cls.ResourceKey(namespace, resource_type, name)

    @classmethod
    def add_resource(cls, resource, overwrite=False):
        key = cls.make_key(type(resource), resource.name, resource.namespace)
        if not overwrite and key in cls.resource_map:
            raise AssertionError(
                f'An object with the key {key!r} already exists in the '
                f'resource map')
        _logger.debug('MockKubernetes.add_resource', resource=resource)
        cls.resource_map[key] = resource

    @classmethod
    @contextlib.contextmanager
    def patch(cls, resources: Iterable[kube.Resource]):
        try:
            _logger.debug(
                'MockKubernetes.patch',
                message='Patching Kubernetes',
                resources=resources
            )
            for resource in resources:
                cls.add_resource(resource)

            patch_kubernetes = mock.patch(
                'k8s_snapshots.kube.Kubernetes',
                cls
            )
            with patch_kubernetes:
                yield
        finally:
            _logger.debug(
                'MockKubernetes.patch',
                message='Cleaning up after patch'
            )
            cls.resource_map.clear()


@contextlib.contextmanager
def mock_kube(resources: Iterable[kube.Resource]):
    """
    Mock the resources available through the `k8s_snapshots.kube.Kubernetes`
    abstraction.

    Parameters
    ----------
    resources

    Returns
    -------
    The `k8s_snapshots.kube.Kubernetes` mock

    """
    with MockKubernetes.patch(resources):
        yield


def make_resource(
        resource_type: Type[kube.Resource],
        name,
        namespace=DEFAULT,
        labels=DEFAULT,
        annotations=DEFAULT,
        spec=DEFAULT,
) -> kube.Resource:
    """
    Create a Kubernetes Resource.
    """

    if namespace is DEFAULT:
        namespace = 'default'

    if annotations is DEFAULT:
        annotations = {}

    api = MagicMock(
        spec=pykube.HTTPClient,
        config=Mock()
    )

    if spec is DEFAULT:
        spec = {}

    obj = {
        'metadata': {
            'name': name,
            'annotations': annotations,
            'selfLink': f'test/{namespace}/{resource_type.endpoint}/{name}'
        },
        'spec': spec,
    }

    if labels is not DEFAULT:
        obj['metadata']['labels'] = labels
    if namespace is not DEFAULT:
        obj['metadata']['namespace'] = namespace

    return resource_type(api, obj)


def make_volume_and_claim(
        ctx,
        volume_name='test-pv',
        claim_name='test-pvc',
        volume_annotations=DEFAULT,
        claim_annotations=DEFAULT,
        claim_namespace=DEFAULT,
        volume_zone_label=DEFAULT,
) -> Tuple[
    pykube.objects.PersistentVolume,
    pykube.objects.PersistentVolumeClaim
]:
    """
    Creates

    """
    if volume_zone_label is DEFAULT:
        volume_zone_label = {LABEL_ZONE_KEY: LABEL_ZONE_VALUE}

    pv = make_resource(
        pykube.objects.PersistentVolume,
        volume_name,
        annotations=volume_annotations,
        labels=volume_zone_label,
        spec={
            'claimRef': {
                'name': claim_name,
                'namespace': claim_namespace,
            },
            'gcePersistentDisk': {
                'pdName': 'test-pd'
            }
        }
    )

    pvc = make_resource(
        pykube.objects.PersistentVolumeClaim,
        claim_name,
        annotations=claim_annotations,
        namespace=claim_namespace,
        spec={
            'volumeName': volume_name,
        }
    )

    return pv, pvc


@pytest.fixture
def fx_volume_zone_label(request):
    return {LABEL_ZONE_KEY: LABEL_ZONE_VALUE}


@pytest.fixture
def fx_annotation_deltas(request):
    deltas = request.getfixturevalue('fx_deltas')
    context = request.getfixturevalue('fx_context')
    return {
        context.config['deltas_annotation_key']: deltas
    }


def spec_gce_persistent_disk(pd_name):
    return {
        'gcePersistentDisk': {
            'pdName': pd_name
        }
    }
