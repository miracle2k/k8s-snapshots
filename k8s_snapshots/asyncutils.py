# Consider: https://github.com/vxgmichel/aiostream

import asyncio

import structlog
from aiochannel import Channel

_logger = structlog.get_logger()


async def run_in_executor(func):
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
    # It seem that is because once we attach a done callback to
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


async def debounce(stream, delay):
    debounced = Channel()
    loop = asyncio.get_event_loop()

    async def iterator():
        scheduled_call = None
        async for item in stream:
            if scheduled_call:
                scheduled_call.cancel()
            scheduled_call = loop.call_later(
                delay,
                lambda: asyncio.ensure_future(debounced.put(item))
            )

    # Read the incoming iterator in a task. If the task fails, close the
    # channel so the iterator below will stop reading.
    task = asyncio.ensure_future(iterator())
    def cb(task):
        if task.exception():
            debounced.close()
    task.add_done_callback(cb)

    async for item in debounced:
        yield item

    task.cancel()
    await asyncio.gather(task)


class StreamReader:
    """Allows iterating over the same iterable multiple times, at the same
    time. That is, while the source iterable is only running multiple times,
    you can consume it with more than one iterator.

    We begin reading from the source when the first iterator starts, and we
    stop once the later iterator leaves.
    """

    def __init__(self, iterable):
        self._task = None
        self.iterable = iterable
        self.channels = []

    async def _iterate_task(self):
        async for item in self.iterable:
            for channel in self.channels:
                await channel.put(item)

    def _ensure_running(self):
        if self._task:
            return
        
        self._task = asyncio.ensure_future(self._iterate_task())
        def cb(task):
            if task.exception():
                for channel in self.channels:
                    channel.close()
        self._task.add_done_callback(cb)

    def _end(self):
        self._task.cancel()

    def iter(self):
        # Return a new iterator
        channel = Channel()
        self.channels.append(channel)
        self._ensure_running()
        return channel
