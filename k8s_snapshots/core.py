#!/usr/bin/env python3
import asyncio

import pykube
from aiochannel import Channel, ChannelEmpty
from aiostream import stream

from k8s_snapshots.context import Context
from k8s_snapshots.kube import (
    watch_resources
)


async def rules_from_volumes(ctx):
    merged_stream = stream.merge(
        watch_resources(ctx, pykube.objects.PersistentVolumeClaim, 1),
        watch_resources(ctx, pykube.objects.PersistentVolume, 3),
    )

    async with merged_stream.stream() as merged_events:
        async for event in merged_events:
            pass


async def daemon(*, loop=None):
    loop = loop or asyncio.get_event_loop()

    schedule_task = asyncio.ensure_future(
        rules_from_volumes(Context({})))

    tasks = [schedule_task]
    await asyncio.gather(*tasks)
