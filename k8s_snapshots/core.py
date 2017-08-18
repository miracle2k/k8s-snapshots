#!/usr/bin/env python3
"""Written in asyncio as a learning experiment. Python because the
backup expiration logic is already in tarsnapper and well tested.

TODO: prevent a backup loop: A failsafe mechanism to make sure we
  don't create more than x snapshots per disk; in case something
  is wrong with the code that loads the exsting snapshots from GCloud.
TODO: Support http ping after every backup.
TODO: Support loading configuration from a configmap.
TODO: We could use a third party resource type, too.
"""
import asyncio
from typing import Union

import pendulum
import pykube
import structlog
from aiochannel import Channel, ChannelEmpty
from aiostream import stream

from k8s_snapshots import events
from k8s_snapshots.asyncutils import combine_latest
from k8s_snapshots.context import Context
from k8s_snapshots.errors import (
    AnnotationNotFound,
    AnnotationError,
    UnsupportedVolume,
    VolumeNotFound
)
from k8s_snapshots.kube import (
    watch_resources,
    get_resource_or_none
)
from k8s_snapshots.rule import rule_from_pv
from k8s_snapshots.snapshot import (
    make_backup,
    get_snapshots,
    determine_next_snapshot
)

_logger = structlog.get_logger()


async def volume_from_resource(
        ctx: Context,
        resource: Union[
            pykube.objects.PersistentVolume,
            pykube.objects.PersistentVolumeClaim,
        ]
) -> pykube.objects.PersistentVolume:
    _log = _logger.new(resource=resource)
    if isinstance(resource, pykube.objects.PersistentVolume):
        return resource
    elif isinstance(resource, pykube.objects.PersistentVolumeClaim):
        pvc = resource

        try:
            volume_name = resource.obj['spec']['volumeName']
        except KeyError as exc:
            raise VolumeNotFound(
                'Could not get volume name from volume claim',
                volume_claim=pvc.obj
            ) from exc

        _log = _log.bind(
            volume_name=volume_name
        )

        _log.debug(
            'Looking for volume',
            key_hints=['volume_name']
        )

        volume = await get_resource_or_none(
            ctx.kube_client,
            pykube.objects.PersistentVolume,
            volume_name,
        )
        if volume is None:
            raise VolumeNotFound(
                f'Could not find volume with name {volume_name!r}',
                volume_claim=pvc.obj,
            )
        return volume
    else:
        raise VolumeNotFound(
            f'It is not possible to get a volume object for an object of type '
            f'{type(resource)}',
            resource=resource,
        )


async def rules_from_volumes(ctx):
    rules = {}

    _logger.debug('volume-events.watch')

    merged_stream = stream.merge(
        watch_resources(ctx, pykube.objects.PersistentVolume, delay=0),
        watch_resources(ctx, pykube.objects.PersistentVolumeClaim, delay=2)
    )

    async with merged_stream.stream() as merged_events:
        async for event in merged_events:
            _log_event = _logger.bind(
                event_type=event.type,
                event_object=event.object.obj,
            )
            _log_event.info(
                events.VolumeEvent.RECEIVED,
                key_hints=[
                    'event_type',
                    'event_object.metadata.name',
                ],
            )
            try:
                volume = await volume_from_resource(ctx, event.object)
            except VolumeNotFound:
                _log_event.exception(
                    events.Volume.NOT_FOUND,
                    key_hints=[
                        'event_type',
                        'event_object.metadata.name',
                    ],
                )
                continue

            volume_name = volume.name
            _log = _logger.new(
                volume_name=volume_name,
                volume_event_type=event.type,
                volume=volume.obj,
            )

            if event.type == 'ADDED' or event.type == 'MODIFIED':
                rule = None
                try:
                    rule = await rule_from_pv(
                        ctx,
                        volume,
                        ctx.config.get('deltas_annotation_key'),
                        use_claim_name=ctx.config.get('use_claim_name'))
                except AnnotationNotFound as exc:
                    _log.info(
                        events.Annotation.NOT_FOUND,
                        key_hints=['volume.metadata.name'],
                        exc_info=exc,
                    )
                except AnnotationError:
                    _log.exception(
                        events.Annotation.ERROR,
                        key_hints=['volume.metadata.name'],
                    )
                except UnsupportedVolume as exc:
                    _log.info(
                        events.Volume.UNSUPPORTED,
                        key_hints=['volume.metadata.name'],
                        exc_info=exc,
                    )

                _log = _log.bind(
                    rule=rule
                )

                if rule:
                    if event.type == 'ADDED' or volume_name not in rules:
                        _log.info(
                            events.Rule.ADDED,
                            key_hints=['rule.name']
                        )
                    else:
                        _log.info(
                            events.Rule.UPDATED,
                            key_hints=['rule.name']
                        )
                    rules[volume_name] = rule
                else:
                    if volume_name in rules:
                        _log.info(
                            events.Rule.REMOVED,
                            key_hints=['volume_name']
                        )
                        rules.pop(volume_name)
            elif event.type == 'DELETED':
                if volume_name in rules:
                    _log.info(
                        events.Rule.REMOVED,
                        key_hints=['volume_name']
                    )
                    rules.pop(volume_name)
            else:
                _log.warning('Unhandled event')

            yield list(rules.values())

        _logger.debug('sync-get-rules.done')


async def get_rules(ctx):
    _log = _logger.new()

    async for rules in rules_from_volumes(ctx):
        _log.debug('get-rules.rules.updated', rules=rules)
        yield rules

    _log.debug('get-rules.done')


async def watch_schedule(ctx, trigger, *, loop=None):
    """Continually yields the next backup to be created.

    It watches two input sources: the rules as defined by
    Kubernetes resources, and the existing snapshots, as returned
    from Google Cloud. If either of them change, a new backup
    is scheduled.
    """
    loop = loop or asyncio.get_event_loop()
    _log = _logger.new()

    rulesgen = get_rules(ctx)

    # TODO: For now, we load the snapshots of a fixed backend, either AWS,
    # or Google, or whatever is globally configured. But in theory different
    # rules could have different backends, so "get_snapshots" would actually
    # need to have a look at the rules itself, and check the snapshots of
    # every applicable rule.
    backend = ctx.get_backend()
    snapgen = get_snapshots(ctx, backend, trigger)

    _log.debug('watch_schedule.start')

    combined = combine_latest(
        rules=rulesgen,
        snapshots=snapgen,
        defaults={'snapshots': None, 'rules': None}
    )

    rules = None

    heartbeat_interval_seconds = ctx.config.get(
        'schedule_heartbeat_interval_seconds'
    )

    async def heartbeat():
        _logger.info(
            events.Rule.HEARTBEAT,
            rules=rules,
        )

        loop.call_later(
            heartbeat_interval_seconds,
            asyncio.ensure_future,
            heartbeat()
        )

    if heartbeat_interval_seconds:
        asyncio.ensure_future(heartbeat())

    async for item in combined:
        rules = item.get('rules')
        snapshots = item.get('snapshots')

        # Never schedule before we have data from both rules and snapshots
        if rules is None or snapshots is None:
            _log.debug(
                'watch_schedule.wait-for-both',
            )
            continue

        yield determine_next_snapshot(snapshots, rules)


async def scheduler(ctx, scheduling_chan, snapshot_reload_trigger):
    """The "when to make a backup schedule" depends on the backup delta
    rules as defined in Kubernetes volume resources, and the existing
    snapshots.

    This simpy observes a stream of 'next planned backup' events and
    sends then to the channel given. Note that this scheduler
    doesn't plan multiple backups in advance. Only ever a single
    next backup is scheduled.
    """
    _log = _logger.new()
    _log.debug('scheduler.start')

    async for schedule in watch_schedule(ctx, snapshot_reload_trigger):
        _log.debug('scheduler.schedule', schedule=schedule)
        await scheduling_chan.put(schedule)


async def backuper(ctx, scheduling_chan, snapshot_reload_trigger):
    """Will take tasks from the given queue, then execute the backup.
    """
    _log = _logger.new()
    _log.debug('backuper.start')

    current_target_time = current_target_rule = None
    while True:
        await asyncio.sleep(0.1)

        try:
            current_target_rule, current_target_time = scheduling_chan.get_nowait()

            # Log a message
            if not current_target_time:
                _log.debug('backuper.no-target')
            else:
                _log.debug(
                    'backuper.next-backup',
                    key_hints=[
                        'rule.name',
                        'target_time',
                    ],
                    rule=current_target_rule,
                    target_time=current_target_time,
                    diff=current_target_time.diff(),
                )
        except ChannelEmpty:
            pass

        if not current_target_time:
            continue

        if pendulum.now('utc') > current_target_time:
            try:
                await make_backup(ctx, current_target_rule)
            finally:
                await snapshot_reload_trigger.put(True)
                current_target_time = current_target_rule = None


async def daemon(config, *, loop=None):
    """Main app; it runs two tasks; one schedules backups, the other
    one executes the.
    """
    loop = loop or asyncio.get_event_loop()

    ctx = Context(config)

    # Using this channel, we can trigger a refresh of the list of
    # disk snapshots in the Google Cloud.
    snapshot_reload_trigger = Channel()

    # The backup task consumes this channel for the next backup task.
    scheduling_chan = Channel()

    schedule_task = asyncio.ensure_future(
        scheduler(ctx, scheduling_chan, snapshot_reload_trigger))
    backup_task = asyncio.ensure_future(
        backuper(ctx, scheduling_chan, snapshot_reload_trigger))

    tasks = [schedule_task, backup_task]

    _logger.debug('Gathering tasks', tasks=tasks)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        _logger.exception(
            'Received CancelledError',
            tasks=tasks
        )

        for task in tasks:
            task.cancel()
            _logger.debug('daemon cancelled task', task=task)

        while True:
            finished, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED)

            _logger.debug(
                'task completed',
                finished=finished,
                pending=pending)

            if not pending:
                _logger.debug('all tasks done')
                raise
