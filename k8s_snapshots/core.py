#!/usr/bin/env python3
"""Written in asyncio as a learning experiment. Python because the
backup expiration logic is already in tarsnapper and well tested.

TODO: prevent a backup loop: A failsafe mechanism to make sure we
  don't create more than x snapshots per disk; in case something
  is wrong with the code that loads the exsting snapshots from GCloud.
"""
import asyncio
from typing import Union

import pendulum
import pykube
import structlog
from aiochannel import Channel, ChannelEmpty
from aiostream import stream

from k8s_snapshots import events
from k8s_snapshots.backends import get_backend
from k8s_snapshots.asyncutils import combine_latest, StreamReader
from k8s_snapshots.context import Context
from k8s_snapshots.errors import (
    AnnotationNotFound,
    AnnotationError,
    UnsupportedVolume,
    VolumeNotFound,
    ConfigurationError,
    DeltasParseError
)
from k8s_snapshots.kube import (
    watch_resources,
    get_resource_or_none,
    SnapshotRule
)
from k8s_snapshots.rule import (
    rule_from_pv, Rule, parse_deltas, rule_name_from_k8s_source)
from k8s_snapshots.snapshot import (
    make_backup,
    get_snapshots,
    determine_next_snapshot
)

_logger = structlog.get_logger()


async def volume_from_pvc(
        ctx: Context,
        resource: pykube.objects.PersistentVolumeClaim
) -> pykube.objects.PersistentVolume:
    """Resolvse a volume claim to the associated volume.
    """
    _log = _logger.new(resource=resource)

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


async def rule_from_resource(
    ctx: Context,
    resource: Union[
        pykube.objects.PersistentVolume,
        pykube.objects.PersistentVolumeClaim,
        SnapshotRule
    ]
) -> Rule:
    """Given a Kubernetes resource, converts it to a snapshot `Rule`
    instance, or returns None.

    How this process works, depends on the resource given.

    - If the resource is a volume, we read the disk and delta info
      from there.

    - If the resource is a volume claim, we look for the volume.

    - A `SnapshotRule` custom resource directly defines the disk.
    """

    _log = _logger.new(
        resource=resource,
    )

    if (isinstance(resource, SnapshotRule)):
        # Validate the backen
        backend_name = resource.obj.get('spec', {}).get('backend')
        try:
            backend = get_backend(backend_name)
        except ConfigurationError as e:
            _log.exception(
                'rule.invalid',
                message=e.message,
                backend=backend_name
            )
            return

        # Validate the deltas
        try:
            deltas_str = resource.obj.get('spec', {}).get('deltas')
            try:
                deltas = parse_deltas(deltas_str)
            except DeltasParseError as exc:
                raise AnnotationError(
                    'Invalid delta string',
                    deltas_str=deltas_str
                ) from exc

            if deltas is None or not deltas:
                raise AnnotationError(
                    'parse_deltas returned invalid deltas',
                    deltas_str=deltas_str,
                    deltas=deltas,
                )
        except AnnotationError:
            _log.exception(
                'rule.invalid',
                key_hints=['volume.metadata.name'],
            )
            return

        # Validate the disk identifier
        disk = resource.obj.get('spec', {}).get('disk')
        try:
            disk = backend.validate_disk_identifier(disk)
        except:
            _log.exception(
                'rule.invalid',
                key_hints=['volume.metadata.name'],
            )
            return

        rule = Rule(
            name=rule_name_from_k8s_source(resource),
            deltas=deltas,
            backend=backend_name,
            disk=disk
        )
        return rule

    if isinstance(resource, pykube.objects.PersistentVolumeClaim):
        try:
            volume = await volume_from_pvc(ctx, resource)
        except VolumeNotFound:
            _log.exception(
                events.Volume.NOT_FOUND,
                key_hints=[
                    'resource.metadata.name',
                ],
            )
            return

    elif isinstance(resource, pykube.objects.PersistentVolume):
        volume = resource

    else:
        raise RuntimeError(f'{resource} is not supported.')

    volume_name = volume.name
    _log = _log.bind(
        volume_name=volume_name,
        volume=volume.obj,
    )

    try:
        rule = await rule_from_pv(
            ctx,
            volume,
            ctx.config.get('deltas_annotation_key'),
            use_claim_name=ctx.config.get('use_claim_name'))
        return rule
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


async def rules_from_volumes(ctx):
    """This generator continuously runs, watching Kubernetes for
    certain resources, consuming changes, and determining which
    snapshot rules have been defined.

    Every value is returns is a list of `Rule` objects, a complete
    set of snapshot rules defined at this point in time. Every set
    of rule objects replaces the previous one.
    """

    rules = {}

    _logger.debug('volume-events.watch')

    merged_stream = stream.merge(
        watch_resources(ctx, pykube.objects.PersistentVolume, delay=0),
        watch_resources(ctx, pykube.objects.PersistentVolumeClaim, delay=2),
        watch_resources(ctx, SnapshotRule, delay=3, allow_missing=True)
    )

    async with merged_stream.stream() as merged_events:
        async for event in merged_events:
            _log = _logger.bind(
                event_type=event.type,
                event_object=event.object.obj,
            )
            _log.info(
                events.VolumeEvent.RECEIVED,
                key_hints=[
                    'event_type',
                    'event_object.metadata.name',
                ],
            )

            rule = await rule_from_resource(ctx, event.object)
            if not rule:
                continue

            _log = _log.bind(
                rule=rule
            )

            key_by = (
                event.object.kind,
                event.object.namespace,
                event.object.name
            )


            if event.type == 'ADDED' or event.type == 'MODIFIED':
                if rule:
                    if event.type == 'ADDED' or key_by not in rules:
                        _log.info(
                            events.Rule.ADDED,
                            key_hints=['rule.name']
                        )
                    else:
                        _log.info(
                            events.Rule.UPDATED,
                            key_hints=['rule.name']
                        )
                    rules[key_by] = rule
                else:
                    if key_by in rules:
                        _log.info(
                            events.Rule.REMOVED,
                            key_hints=['volume_name']
                        )
                        rules.pop(key_by)

            elif event.type == 'DELETED':
                if key_by in rules:
                    _log.info(
                        events.Rule.REMOVED,
                        key_hints=['volume_name']
                    )
                    rules.pop(volume_name)
            else:
                _log.warning('Unhandled event')

            # We usually have duplicate disks within in `rules`,
            # which is indexed by resource kind. One reason is we
            # watching both PVCs and PVs, and a PVC/PV pair resolve
            # to the same disk. It is also possible that custom rules
            # the user defined contain duplicates. Let's make sure
            # we only have one rule for every disk. Note that which
            # one we pick is undefined.
            #
            # In the (internal) case of PV/PVC pairs it does't matter,
            # since our code is written thus: The rule always references
            # the volume, and we always check the volume, then the claim
            # for deltas. The behaviour for this case is well-defined.
            unique_rules = {rule.disk: rule for rule in rules.values()}.values()
            # TODO: Log in a different place, in a debounced way
            #_logger.info('sync-get-rules.yield', rule_count=len(unique_rules))
            yield list(unique_rules)

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


    rules_reader = StreamReader(get_rules(ctx))
    snapgen = get_snapshots(ctx, rules_reader.iter(), trigger)

    _log.debug('watch_schedule.start')

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

    combined = combine_latest(
        rules=rules_reader.iter(),
        snapshots=snapgen,
        defaults={'snapshots': None, 'rules': None}
    )

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
