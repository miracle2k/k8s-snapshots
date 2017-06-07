# Consider: https://github.com/vxgmichel/aiostream

import asyncio
from aiochannel import Channel


async def exec(func):
    return await asyncio.get_event_loop().run_in_executor(None, func)


async def combine(**generators):
    """Given a bunch of async generators, merges the events from
    all of them. Each should have a name, i.e. `foo=gen, bar=gen`.
    """
    combined = Channel()
    async def listen_and_forward(name, generator):
        async for value in generator:
            await combined.put({name: value})
    tasks = []
    for name, generator in generators.items():
        task = asyncio.ensure_future(listen_and_forward(name, generator))
        # When task one or fails, close channel so that later our
        # iterator stops reading.
        def cb(task):
            if task.exception():
                combined.close()
        task.add_done_callback(cb)
        tasks.append(task)

    # This one will stop when either all generators are exhaused,
    # or any one of the fails.
    async for item in combined:
        yield item

    # TODO: gather() can hang, and the task cancellation doesn't
    # really work. Happens if one of the generators has an error.
    # It seem that is bevause once we attach a done callback to
    # the task, gather() doesn't handle the exception anymore??
    # Any tasks that are still running at this point, cancel them.
    for task in tasks:
        task.cancel()
    # Will consume any task exceptions
    await asyncio.gather(*tasks)


async def combine_latest(defaults=None, **generators):
    """Like "combine", but always includes the latest value from
    every generator.
    """
    current = defaults.copy() if defaults else {}
    async for value in combine(**generators):
        current.update(value)
        yield current


async def iterate_in_executor(sync_iter, *args):
    """run_in_executor returns a future. But what if the function
    we call is supposed to return values iteratively?
    """
    loop = asyncio.get_event_loop()
    channel = Channel()
    def forward_iter(*a):
        try:
            # TODO: We are looking for a solution to stop this
            # if the channel is closed. Should this thread use it's
            # own event loop where we can use await?
            for value in sync_iter(*a):
                asyncio.ensure_future(channel.put(value), loop=loop)
        finally:
            channel.close()
    result = asyncio.get_event_loop().run_in_executor(None, forward_iter, *args)
    async for item in channel:
        yield item

    # Any exceptions would be retrieved here
    await result