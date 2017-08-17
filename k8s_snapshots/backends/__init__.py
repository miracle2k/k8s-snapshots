from importlib import import_module
import pykube.objects


BACKENDS = ['google']


def get_backends():
    for name in BACKENDS:
        try:
            backend = import_module('k8s_snapshots.backends.%s' % name)
        except ImportError:
            continue
        yield name, backend


def get_backend(name: str):
    return import_module('k8s_snapshots.backends.%s' % name)


def find_backend_for_volume(volume: pykube.objects.PersistentVolume):
    """
    See if we have a provider that supports this volume.
    """
    for name, backend in get_backends():
        if backend.supports_volume(volume):
            return name, backend

