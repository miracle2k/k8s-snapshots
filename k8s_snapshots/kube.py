import asyncio
import threading
from aiochannel import Channel


async def watch_resources(
        ctx,
        resource_type,
        timeout,
        *,
        loop=None
):

    loop = loop or asyncio.get_event_loop()
    channel = Channel()

    #await asyncio.sleep(timeout)

    def worker():
        try:
            client_factory = ctx.kube_client
            api = client_factory()
            sync_iterator = resource_type.objects(
                api).watch().object_stream()

            for event in sync_iterator:
                print('got an event', event, resource_type)
                loop.call_soon_threadsafe(channel.put_nowait, event)
        finally:
            channel.close()

    thread = threading.Thread(
        target=worker,
        daemon=True,
    )
    thread.start()

    async for _ in channel:
        yield True

