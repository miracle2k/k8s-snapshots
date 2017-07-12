import asyncio
import threading
from typing import Optional, Iterable, NamedTuple, AsyncGenerator, Any, TypeVar

import pykube
import structlog
from aiochannel import Channel

from k8s_snapshots.context import Context

_logger = structlog.get_logger(__name__)

KubeResourceType = TypeVar(
    'KubeResourceType',
)


def get_resource_or_none_sync(
        client: pykube.HTTPClient,
        resource_type: type(KubeResourceType),
        name: str,
        namespace: Optional[str]=None,
) -> Optional[KubeResourceType]:
    resource_query = resource_type.objects(client)
    if namespace is not None:
        resource_query = resource_query.filter(
            namespace=namespace
        )

    return resource_query.get_or_none(name=name)


async def get_resource_or_none(
        ctx: Context,
        resource_type: type(pykube.objects.APIObject),
        name: str,
        namespace: Optional[str]=None,
        *,
        loop=None
) -> Optional[pykube.objects.APIObject]:
    loop = loop or asyncio.get_event_loop()

    def _get():
        return get_resource_or_none_sync(
            client=ctx.kube_client(),
            resource_type=resource_type,
            name=name,
            namespace=namespace,
        )

    return await loop.run_in_executor(
        None,
        _get,
    )


def watch_resources_sync(
        client: pykube.HTTPClient,
        resource_type: type(pykube.objects.APIObject),
) -> Iterable:
    return resource_type.objects(client).watch().object_stream()


async def watch_resources(
        ctx: Context,
        resource_type: type(KubeResourceType),
        *,
        loop=None
) -> AsyncGenerator[KubeResourceType, None]:
    loop = loop or asyncio.get_event_loop()
    _log = _logger.bind(
        resource_type_name=resource_type.__name__,
    )
    channel = Channel()

    def worker():
        try:
            _log.debug('watch-resources.worker.start')
            sync_iterator = watch_resources_sync(
                ctx.kube_client(),
                resource_type
            )
            for event in sync_iterator:
                # only put_nowait seems to cause SIGSEGV
                loop.call_soon_threadsafe(channel.put_nowait, event)
        except:
            _log.exception('watch-resources.worker.error')
        finally:
            _log.debug('watch-resources.worker.finalized')
            channel.close()

    thread = threading.Thread(
        target=worker,
        daemon=True,
    )
    thread.start()

    async for event in channel:
        yield event

    _log.debug('watch-resources.done')

