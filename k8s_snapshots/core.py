#!/usr/bin/env python3
"""Written in asyncio as a learning experiment. Python because the
backup expiration logic is already in tarsnapper and well tested.
"""
import asyncio
import re
import os
import threading
from datetime import timedelta
from typing import Optional, Dict, Any

import attr
import pendulum
import pykube
import structlog
from aiochannel import Channel, ChannelEmpty
from tarsnapper.config import parse_deltas, ConfigError
from tarsnapper.expire import expire

from k8s_snapshots import errors
from k8s_snapshots.asyncutils import combine_latest, exec
# TODO: prevent a backup loop: A failsafe mechanism to make sure we
#   don't create more than x snapshots per disk; in case something
#   is wrong with the code that loads the exsting snapshots from GCloud.
# TODO: Support http ping after every backup.
# TODO: Support loading configuration from a configmap.
# TODO: We could use a third party resource type, too.
from k8s_snapshots.context import Context

_logger = structlog.get_logger()


@attr.s(slots=True)
class Rule:
    """
    A rule describes how and when to make backups.
    """

    name = attr.ib()
    namespace = attr.ib()
    deltas = attr.ib()
    deltas_unparsed = attr.ib()
    gce_disk = attr.ib()
    gce_disk_zone = attr.ib()
    claim_name = attr.ib()

    @property
    def pretty_name(self):
        return self.claim_name or self.name

    def to_dict(self) -> Dict[str, Any]:
        """ Helper, returns attr.asdict(self) """
        return attr.asdict(self)

    def __str__ (self):
        return self.name


def filter_snapshots_by_rule(snapshots, rule):
    def match_disk(snapshot):
        url_part = '/zones/{zone}/disks/{name}'.format(
            zone=rule.gce_disk_zone, name=rule.gce_disk)
        return snapshot['sourceDisk'].endswith(url_part)
    return filter(match_disk, snapshots)


def determine_next_snapshot(snapshots, rules):
    """
    Given a list of snapshots, and a list of rules, determine the next snapshot
    to be made.

    Returns a 2-tuple (rule, target_datetime)
    """
    next_rule = None
    next_timestamp = None

    for rule in rules:
        _log = _logger.new(rule=rule.to_dict())
        # Find all the snapshots that match this rule
        filtered = filter_snapshots_by_rule(snapshots, rule)
        # Rewrite the list to snapshot
        filtered = map(lambda s: pendulum.parse(s['creationTimestamp']), filtered)
        # Sort by timestamp
        filtered = sorted(filtered, reverse=True)
        filtered = list(filtered)

        # There are no snapshots for this rule; create the first one.
        if not filtered:
            _logger.debug('No snapshot yet for rule, it will be next')
            next_rule = rule
            next_timestamp = pendulum.now('utc') + timedelta(seconds=10)
            break

        target = filtered[0] + rule.deltas[0]
        _logger.debug('Next snapshot found', target=target)
        if not next_timestamp or target < next_timestamp:
            next_rule = rule
            next_timestamp = target

    return next_rule, next_timestamp


def deltas_from_gce_pd(volume, api, annotation_key) -> Optional[str]:
    """
    Get the delta string based on a GCE PD volume.

    Parameters
    ----------

    volume : pykube.objects.PersistentVolume
        GCE PD Volume.
    api : pykube.HTTPClient
        Kubernetes client.
    annotation_key : str
        The annotation key to get deltas from..

    Returns
    -------
    The delta string

    """
    deltas_str = volume.annotations.get(annotation_key)

    _log = _logger.new(
        annotation_key=annotation_key,
        volume_name=volume.name,
        volume=volume.obj,
    )

    if deltas_str is not None and deltas_str:
        _log.info(
            'Found delta annotation for volume',
            deltas_str=deltas_str,
        )
        return deltas_str

    _log.debug(
        'Volume has no deltas annotation',
        deltas_str=deltas_str
    )

    # If volume is not annotated, attempt ot read deltas from
    # PersistentVolumeClaim referenced in volume.claimRef

    claim_ref = volume.obj['spec'].get('claimRef')
    _log = _log.bind(claim_ref=claim_ref)

    if claim_ref is None:
        _log.debug(
            'Volume has no claimRef',
        )
        return

    volume_claim = (
        pykube.objects.PersistentVolumeClaim.objects(api)
        .filter(namespace=claim_ref['namespace'])
        .get_or_none(name=claim_ref['name'])
    )
    _log = _log.bind(
        volume_claim_name=volume_claim.name,
        volume_claim=volume_claim.obj,
    )

    if volume_claim is None:
        _log.debug(
            'Volume claim does not exist',
        )
        return

    deltas_str = volume_claim.annotations.get(annotation_key)

    if deltas_str is not None and deltas_str:
        _log.debug(
            'Found deltas for volume claim',
            deltas_str=deltas_str,
        )
        return deltas_str

    _log.debug(
        'Volume claim has no deltas annotation',
        deltas_str=deltas_str,
    )
    return


def rule_from_pv(volume, api, deltas_annotation_key, use_claim_name=False):
    """Given a persistent volume object, create a backup role
    object. Can return None if this volume is not configured for
    backups, or is not suitable.

    Parameters

    `use_claim_name` - if the persistent volume is bound, and it's
    name is auto-generated, then prefer to use the name of the claim
    for the snapshot.
    """
    _log = _logger.new(
        volume_name=volume.name,
        volume=volume.obj,
    )

    provider = volume.annotations.get('pv.kubernetes.io/provisioned-by')
    _log = _log.bind(provider=provider)
    if provider != 'kubernetes.io/gce-pd':
        _log.debug('Volume not a GCE persistent disk', volume=volume)
        return

    deltas_unparsed = deltas_from_gce_pd(volume, api, deltas_annotation_key)
    _log = _log.bind(deltas_str=deltas_unparsed)

    if deltas_unparsed is None:
        _log.info(
            'rule.from-pv.deltas-missing',
            key_hint='volume.metadata.name')
        return

    try:
        _log.debug('Parsing deltas', unparsed_deltas=deltas_unparsed)
        deltas = parse_deltas(deltas_unparsed)

        if not deltas:
            raise ConfigError(
                'parse_deltas did not raise, but returned invalid deltas: '
                '{!r}'.format(deltas)
            )
    except ConfigError as e:
        _log.exception(
            'rule.from-pv.deltas-invalid',
            error=e,
        )
        return

    gce_disk = volume.obj['spec']['gcePersistentDisk']['pdName']

    # How can we know the zone? In theory, the storage class can
    # specify a zone; but if not specified there, K8s can choose a
    # random zone within the master region. So we really can't trust
    # that value anyway.
    # There is a label that gives a failure region, but labels aren't
    # really a trustworthy source for this.
    # Apparently, this is a thing in the Kubernetes source too, see:
    # getDiskByNameUnknownZone in pkg/cloudprovider/providers/gce/gce.go,
    # e.g. https://github.com/jsafrane/kubernetes/blob/2e26019629b5974b9a311a9f07b7eac8c1396875/pkg/cloudprovider/providers/gce/gce.go#L2455
    gce_disk_zone = volume.labels.get('failure-domain.beta.kubernetes.io/zone')

    claim_name = None
    if use_claim_name and volume.obj['spec'].get('claimRef'):
        if volume.annotations.get('kubernetes.io/createdby') == 'gce-pd-dynamic-provisioner':
            ref = volume.obj['spec'].get('claimRef')
            claim_name = f"{ref['namespace']}--{ref['name']}"

    rule = Rule(
        name=volume.name,
        namespace=volume.namespace,
        deltas=deltas,
        deltas_unparsed=deltas_unparsed,
        gce_disk=gce_disk,
        gce_disk_zone=gce_disk_zone,
        claim_name=claim_name,
    )

    _log.info('rule.from-pv', key_hint='volume.metadata.name', rule=rule.to_dict())

    return rule


def sync_get_rules(ctx):
    rules = {}
    api = ctx.make_kubeclient()

    _logger.debug('Observe persistent volume stream')
    stream = pykube.objects.PersistentVolume.objects(api).watch().object_stream()

    for event in stream:
        volume_name = event.object.name
        _log = _logger.new(
            volume_name=volume_name,
            volume_event_type=event.type,
            volume=event.object.obj,
        )

        _log.info('volume-event.received')

        if event.type == 'ADDED' or event.type == 'MODIFIED':
            rule = rule_from_pv(
                event.object,
                api,
                ctx.config.get('deltas_annotation_key'),
                use_claim_name=ctx.config.get('use_claim_name'))

            _log.info('rule.from-volume')

            if rule:
                _log.bind(
                    rule=rule.to_dict()
                )
                if event.type == 'ADDED' or volume_name not in rules:
                    _log.info(
                        'rule.add',
                        deltas_str=rule.deltas_unparsed
                    )
                else:
                    _log.info('rule.update')
                rules[volume_name] = rule
            else:
                if volume_name in rules:
                    _log.info('rule.delete.missing-deltas')
                rules.pop(volume_name, False)
        elif event.type == 'DELETED':
            _log.info('rule.delete.volume-removed')
            rules.pop(volume_name, False)
        else:
            _log.warning('Unhandled event')

        yield list(rules.values())


async def get_rules(ctx):

    channel = Channel()
    _log = _logger.new(channel=channel)

    _log.info('rules.getting')

    def worker():
        try:
            _log.debug('Iterating in thread')
            for value in sync_get_rules(ctx):
                channel.put(value)
        finally:
            _log.debug('Closing channel')
            channel.close()

    thread = threading.Thread(
        target=worker,
        name='get_rules',
        daemon=True
    )
    _log = _log.bind(thread=thread)

    _log.debug('Starting iterator thread')
    thread.start()

    try:
        async for item in channel:
            _log.debug('Waiting for new item')
            yield ctx.config.get('rules') + item

    except asyncio.CancelledError:
        _log.exception('Cancelled worker')
    except Exception:
        _log.exception('Unhandled exception while iterating')
    finally:
        _log.debug('Iterator exiting')


async def load_snapshots(ctx):
    r = await exec(ctx.gcloud.snapshots().list(project=ctx.config['gcloud_project']).execute)
    return r.get('items', [])


async def get_snapshots(ctx, reload_trigger):
    """Query the existing snapshots from Google Cloud.

    If the channel "reload_trigger" contains any value, we
    refresh the list of snapshots. This will then cause the
    next backup to be scheduled.
    """
    yield await load_snapshots(ctx)
    async for _ in reload_trigger:
        yield await load_snapshots(ctx)


async def watch_schedule(ctx, trigger):
    """Continually yields the next backup to be created.

    It watches two input sources: the rules as defined by
    Kubernetes resources, and the existing snapshots, as returned
    from Google Cloud. If either of them change, a new backup
    is scheduled.
    """

    rulesgen = get_rules(ctx)
    snapgen = get_snapshots(ctx, trigger)
    combined = combine_latest(
        rules=rulesgen,
        snapshots=snapgen,
        defaults={'snapshots': None, 'rules': None})

    try:
        async for item in combined:
            rules = item.get('rules')
            snapshots = item.get('snapshots')

            # Never schedule before we have data from both rules and snapshots
            if rules is None or snapshots is None:
                continue

            yield determine_next_snapshot(snapshots, rules)
    except asyncio.CancelledError:
        _logger.exception('Caught CancelledError while watching schedule')


async def make_backup(ctx, rule):
    """Execute a single backup job.

    1. Create the snapshot
    2. Wait until the snapshot is finished.
    3. Expire old snapshots
    """
    snapshot_time_str = re.sub(
        r'[^a-z0-9-]', '-',
        pendulum.now('utc').format(ctx.config['snapshot_datetime_format']),
        flags=re.IGNORECASE)
    snapshot_name = f'{rule.pretty_name}-{snapshot_time_str}'

    _log = _logger.new(
        snapshot_name=snapshot_name,
        rule=rule.to_dict()
    )

    try:
        _log.info('snapshot.start')
        result = await exec(ctx.gcloud.disks().createSnapshot(
            disk=rule.gce_disk,
            project=ctx.config['gcloud_project'],
            zone=rule.gce_disk_zone,
            body={"name": snapshot_name}).execute)
    except Exception as exc:
        _log.exception('snapshot.start.error')
        raise errors.SnapshotCreateError('Call to API raised an error') from exc

    _log = _log.bind(create_snapshot=result)

    _log.debug('snapshot.started')

    # Immediately after creating the snapshot, it sometimes seems to
    # take some seconds before it can be queried.
    await asyncio.sleep(10)

    _log.debug('Waiting for snapshot to be ready')
    while result['status'] in ('PENDING', 'UPLOADING', 'CREATING'):
        await asyncio.sleep(2)
        _log.debug('snapshot.status.poll')
        result = await exec(ctx.gcloud.snapshots().get(
            snapshot=snapshot_name,
            project=ctx.config['gcloud_project']).execute)
        _log.debug('snapshot.status.polled', result)

    if not result['status'] == 'READY':
        _log.error('snapshot.error', result=result)
        return

    _log.info('snapshot.created', last_result=result)

    await expire_snapshots(ctx, rule)


async def expire_snapshots(ctx, rule: Rule):
    """
    Expire existing snapshots for the rule.
    """
    _log = _logger.new(
        rule=rule.to_dict(),
    )
    _log.info('Expire existing snapshots')

    snapshots = await load_snapshots(ctx)
    snapshots = filter_snapshots_by_rule(snapshots, rule)
    snapshots = {s['name']: pendulum.parse(s['creationTimestamp']) for s in snapshots}

    to_keep = expire(snapshots, rule.deltas)
    _log.info('ex',
        len(snapshots), len(to_keep))
    for snapshot_name in snapshots:
        _log = _log.bind(
            snapshot_name=snapshot_name,
        )
        if snapshot_name in to_keep:
            _log.debug('expire.keep')
            continue

        if snapshot_name not in to_keep:
            _log.info('snapshot.expire')
            result = await exec(ctx.gcloud.snapshots().delete(
                snapshot=snapshot_name,
                project=ctx.config['gcloud_project']).execute)
            _log.info('snapshot.expired', result=result)


async def scheduler(ctx, scheduling_chan, snapshot_reload_trigger):
    """The "when to make a backup schedule" depends on the backup delta
    rules as defined in Kubernetes volume resources, and the existing
    snapshots.

    This simpy observes a stream of 'next planned backup' events and
    sends then to the channel given. Note that this scheduler
    doesn't plan multiple backups in advance. Only ever a single
    next backup is scheduled.
    """

    _logger.info('Started scheduler task')

    async for schedule in watch_schedule(ctx, snapshot_reload_trigger):
        _logger.debug('scheduler.updated', schedule=schedule)
        await scheduling_chan.put(schedule)


async def backuper(ctx, scheduling_chan, snapshot_reload_trigger):
    """Will take tasks from the given queue, then execute the backup.
    """
    _log = _logger.new()
    _log.info('backuper.start')

    current_target_time = current_target_rule = None
    while True:
        await asyncio.sleep(0.1)

        try:
            current_target_rule, current_target_time = scheduling_chan.get_nowait()

            # Log a message
            if not current_target_time:
                _logger.info('backuper.no-target')
            else:
                _logger.info(
                    'backuper.next-backup',
                    rule=current_target_rule.to_dict(),
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


def read_volume_config():
    """Read the volume configuration from the environment
    """
    def read_volume(name):
        _log = _logger.new(
            volume_name=name,
        )
        env_name = name.replace('-', '_').upper()
        deltas_str = os.environ.get('VOLUME_{}_DELTAS'.format(env_name))
        if not deltas_str:
            raise ConfigError('A volume {} was defined, but {} is not set'.format(name, env_name))

        zone = os.environ.get('VOLUME_{}_ZONE'.format(env_name))
        if not zone:
            raise ConfigError('A volume {} was defined, but {} is not set'.format(name, env_name))

        _log = _log.bind(
            deltas_str=deltas_str,
            zone=zone,
        )

        rule = Rule(
            name=name,
            namespace='',
            deltas=parse_deltas(deltas_str),
            deltas_unparsed=deltas_str,
            gce_disk=name,
            gce_disk_zone=zone,
        )

        _log.info('rule.from-env', rule=rule.to_dict())

        return rule

    volumes = filter(bool, map(lambda s: s.strip(), os.environ.get('VOLUMES', '').split(',')))
    config = {}
    config['rules'] = list(filter(bool, map(read_volume, volumes)))
    return config

