import asyncio
import threading
from typing import (
    Optional,
    Iterable,
    AsyncGenerator,
    TypeVar,
    Type,
    NamedTuple, Callable)

import pykube
import structlog
from aiochannel import Channel

from k8s_snapshots.context import Context

_logger = structlog.get_logger(__name__)

Resource = TypeVar(
    'Resource',
    bound=pykube.objects.APIObject,
)

ClientFactory = Callable[[], pykube.HTTPClient]

# Copy of a locally-defined namedtuple in
# pykube.query.WatchQuery.object_stream()
_WatchEvent = NamedTuple(
    '_WatchEvent',
    [
        ('type', str),
        ('object', Resource),
    ]
)


class Kubernetes:
    """
    Allows for easier mocking of Kubernetes resources.
    """
    def __init__(
            self,
            client_factory: Optional[ClientFactory]=None
    ):
        """

        Parameters
        ----------
        client_factory
            Used in threaded operations to create a local
            :any:`pykube.HTTPClient` instance.
        """
        # Used for threaded operations
        self.client_factory = client_factory

    def get_or_none(
            self,
            resource_type: Type[Resource],
            name: str,
            namespace: Optional[str]=None,
    ) -> Optional[Resource]:
        """
        Sync wrapper for :any:`pykube.query.Query().get_or_none`
         """
        resource_query = resource_type.objects(self.client_factory())
        if namespace is not None:
            resource_query = resource_query.filter(
                namespace=namespace
            )

        return resource_query.get_or_none(name=name)

    def watch(
            self,
            resource_type: Type[Resource],
    ) -> Iterable[_WatchEvent]:
        """
        Sync wrapper for :any:`pykube.query.Query().watch().object_stream()`
        """
        return resource_type.objects(self.client_factory()).watch().object_stream()


def get_resource_or_none_sync(
        client_factory: ClientFactory,
        resource_type: Type[Resource],
        name: str,
        namespace: Optional[str]=None,
) -> Optional[Resource]:
    return Kubernetes(client_factory).get_or_none(
        resource_type,
        name,
        namespace,
    )


async def get_resource_or_none(
        client_factory: ClientFactory,
        resource_type: Type[Resource],
        name: str,
        namespace: Optional[str]=None,
        *,
        loop=None
) -> Optional[Resource]:
    loop = loop or asyncio.get_event_loop()

    def _get():
        return get_resource_or_none_sync(
            client_factory=client_factory,
            resource_type=resource_type,
            name=name,
            namespace=namespace,
        )

    return await loop.run_in_executor(
        None,
        _get,
    )


def watch_resources_sync(
        client_factory: ClientFactory,
        resource_type: pykube.objects.APIObject,
) -> Iterable:
    return Kubernetes(client_factory).watch(
        resource_type=resource_type
    )


async def watch_resources(
        ctx: Context,
        resource_type: Resource,
        *,
        delay: int,
        loop=None
) -> AsyncGenerator[_WatchEvent, None]:
    """ Asynchronously watch Kubernetes resources """
    async_gen = _watch_resources_thread_wrapper(
        ctx.kube_client,
        resource_type,
        loop=loop
    )

    # Workaround a race condition in pykube:
    # https: // github.com / kelproject / pykube / issues / 138
    await asyncio.sleep(delay)

    async for item in async_gen:
        yield item


async def _watch_resources_thread_wrapper(
        client_factory: Callable[[], pykube.HTTPClient],
        resource_type: Type[Resource],
        *,
        loop=None
) -> AsyncGenerator[_WatchEvent, None]:
    """ Async wrapper for pykube.watch().object_stream() """
    loop = loop or asyncio.get_event_loop()
    _log = _logger.bind(
        resource_type_name=resource_type.__name__,
    )
    channel = Channel()

    def worker():
        try:
            _log.debug('watch-resources.worker.start')
            sync_iterator = watch_resources_sync(
                client_factory=client_factory,
                resource_type=resource_type
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

    async for channel_event in channel:
        yield channel_event

    _log.debug('watch-resources.done')

