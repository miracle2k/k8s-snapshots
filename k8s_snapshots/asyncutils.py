# Consider: https://github.com/vxgmichel/aiostream

import asyncio

import structlog
from aiochannel import Channel

_logger = structlog.get_logger()


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


