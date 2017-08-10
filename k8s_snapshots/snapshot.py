import asyncio
import inspect
from datetime import timedelta
from typing import Optional, Dict, Tuple, List, Iterable, Callable, Union, \
    Awaitable, Container

import aiohttp
import pendulum
import re
import structlog
from googleapiclient.discovery import Resource as GoogleResource
from tarsnapper.expire import expire

from k8s_snapshots import events, errors, serialize
from k8s_snapshots.asyncutils import run_in_executor
from k8s_snapshots.context import Context
from k8s_snapshots.rule import Rule

_logger = structlog.get_logger(__name__)


async def expire_snapshots(ctx, rule: Rule):
    """
    Expire existing snapshots for the rule.
    """
    _log = _logger.new(
        rule=rule,
    )
    _log.debug('snapshot.expired')

    snapshots = await load_snapshots(ctx)
    snapshots = filter_snapshots_by_rule(snapshots, rule)
    snapshots = {s['name']: parse_creation_timestamp(s) for s in snapshots}

    to_keep = expire(snapshots, rule.deltas)
    expired_snapshots = []
    kept_snapshots = []

    for snapshot_name, snapshot_time_created in snapshots.items():
        _log = _log.new(
            snapshot_name=snapshot_name,
            snapshot_time_created=snapshot_time_created,
            key_hints=[
                'snapshot_name',
                'snapshot_time_created',
            ]
        )
        if snapshot_name in to_keep:
            _log.debug('expire.keep')
            kept_snapshots.append(snapshot_name)
            continue

        if snapshot_name not in to_keep:
            _log.debug('snapshot.expiring')
            result = await run_in_executor(
                ctx.gcloud().snapshots().delete(
                    snapshot=snapshot_name,
                    project=ctx.config['gcloud_project']
                ).execute
            )
            expired_snapshots.append(snapshot_name)

    _log.info(
        events.Snapshot.EXPIRED,
        key_hint='snapshot_name',
        snapshots={
            'expired': expired_snapshots,
            'kept': kept_snapshots,
        }
    )


async def make_backup(ctx, rule):
    """Execute a single backup job.

    1. Create the snapshot
    2. Wait until the snapshot is finished.
    3. Expire old snapshots
    """
    snapshot_name = new_snapshot_name(ctx, rule)

    _log = _logger.new(
        snapshot_name=snapshot_name,
        rule=rule
    )

    try:

        # Returns a ZoneOperation: {kind: 'compute#operation',
        # operationType: 'createSnapshot', ...}.
        # Google's documentation is confusing regarding this, since there's two
        # tables of payload parameter descriptions on the page, one of them
        # describes the input parameters, but contains output-only parameters,
        # the correct table can be found at
        # https://cloud.google.com/compute/docs/reference/latest/disks/createSnapshot#response
        # snapshot_operation = await run_in_executor(
        #     gcloud.disks().createSnapshot(
        #         disk=rule.gce_disk,
        #         project=ctx.config['gcloud_project'],
        #         zone=rule.gce_disk_zone,
        #         body=request_body
        #     ).execute
        # )
        snapshot_operation = await create_snapshot(
            ctx,
            rule.gce_disk,
            rule.gce_disk_zone,
            snapshot_name,
            snapshot_description=serialize.dumps(rule),
        )

        _log.debug(
            'snapshot.operation-started',
            key_hints=['create_snapshot_operation.status'],
            snapshot_operation=snapshot_operation
        )

        # Wait for the createSnapshot operation to leave the 'PENDING' and
        # 'RUNNING' states.
        # A ZoneOperation can have a state of 'PENDING', 'RUNNING', or 'DONE'
        snapshot_operation = await poll_for_status(
            snapshot_operation,
            lambda: get_zone_operation(
                ctx,
                rule.gce_disk_zone,
                snapshot_operation['name']
            ),
            retry_for=('PENDING', 'RUNNING')
        )

    except Exception as exc:
        _log.exception(
            events.Snapshot.ERROR,
            key_hints=['snapshot_name', 'rule.name']
        )
        raise errors.SnapshotCreateError(
            'Error creating snapshot'
        ) from exc

    if not snapshot_operation['status'] == 'DONE':
        raise errors.SnapshotCreateError(
            f'Unexpected ZoneOperation status: {snapshot_operation["status"]} '
            f'expected "DONE"',
            snapshot_operation=snapshot_operation,
        )

    snapshot = await get_snapshot(ctx, snapshot_name)

    await set_snapshot_labels(
        ctx,
        snapshot,
        snapshot_labels(ctx),
    )

    _log.debug('Waiting for snapshot to be ready')

    snapshot = await poll_for_status(
        snapshot,
        lambda: get_snapshot(ctx, snapshot_name),
        retry_for=('CREATING', 'UPLOADING'),
    )

    if not snapshot['status'] == 'READY':
        _log.error(
            events.Snapshot.ERROR,
            snapshot=snapshot,
            key_hints=['snapshot_name', 'rule.name'],
        )
        return

    _log.info(
        events.Snapshot.CREATED,
        snapshot=snapshot,
        key_hints=['snapshot_name', 'rule.name'],
    )

    ping_url = ctx.config.get('ping_url')
    if ping_url:
        with aiohttp.ClientSession() as session:
            response = await session.request('GET', ping_url)
            _log.info(
                events.Ping.SENT,
                status=response.status,
                url=ping_url,
            )

    await expire_snapshots(ctx, rule)


async def create_snapshot(
        ctx: Context,
        disk_name: str,
        disk_zone: str,
        snapshot_name: str,
        snapshot_description: str,
        *,
        gcloud: Optional[GoogleResource]=None
) -> Dict:
    gcloud = gcloud or ctx.gcloud()
    _log = _logger.new(
        disk_name=disk_name,
        disk_zone=disk_zone,
        snapshot_name=snapshot_name,
        snapshot_description=snapshot_description
    )

    request_body = {
        'name': snapshot_name,
        'description': snapshot_description
    }
    # TODO
    _log.info(
        events.Snapshot.START,
        key_hints=['snapshot_name', 'rule.name'],
        request=request_body,
    )
    return await run_in_executor(
        gcloud.disks().createSnapshot(
            disk=disk_name,
            project=ctx.config['gcloud_project'],
            zone=disk_zone,
            body=request_body
        ).execute
    )


async def poll_for_status(
        resource: Dict,
        refresh_func: Callable[..., Union[Dict, Awaitable[Dict]]],
        retry_for: Container[str],
        status_key: str='status',
        sleep_time: int=1,
):
    """
    Refreshes a resource using ``refresh_func`` until ``resource[status_key]`
    is not one of the values in ``retry_for``.

    Parameters
    ----------
    resource
        The initial version of the resource, if ``resource[status_key]``
        already is `not in` ``retry_for``, this method will return the resource
        as-is without polling.
    refresh_func
        Callable that returns either

        -   The new version of the resource.
        -   An awaitable for the new version of the resource.
    retry_for
        A list of statuses to retry for.
    status_key
    sleep_time
        The time, in seconds, to sleep for between calls.

    Returns
    -------

    """
    _log = _logger.new(
        status_key=status_key
    )
    refresh_count = 0
    time_start = pendulum.now()

    while resource[status_key] in retry_for:
        await asyncio.sleep(sleep_time)  # Sleep first

        resource = refresh_func()
        # If refresh_func returned an awaitable, await it
        if inspect.isawaitable(resource):
            _log.debug(
                'poll-for-status.awaiting-resource',
            )
            resource = await resource

        refresh_count += 1

        _log.debug(
            'poll-for-status.refreshed',
            key_hints=[
                'initial_resource.selfLink',
                f'resource.{status_key}'
            ],
            resource=resource,
            refresh_count=refresh_count
        )

    time_taken = pendulum.now() - time_start

    _log.debug(
        'poll-for-status.done',
        key_hints=[
            f'resource.{status_key}',
            'refresh_count',
            'time_taken',
        ],
        resource=resource,
        time_taken=time_taken,
        refresh_count=refresh_count,
        time_start=time_start,
    )

    return resource


def snapshot_author_label(ctx: Context) -> Tuple[str, str]:
    return (
        ctx.config['snapshot_author_label_key'],
        ctx.config['snapshot_author_label']
    )


def snapshot_labels(ctx: Context) -> Dict:
    return dict([snapshot_author_label(ctx)])


async def set_snapshot_labels(
        ctx: Context,
        snapshot: Dict,
        labels: Dict,
        gcloud: Optional[GoogleResource]=None,
):
    _log = _logger.new(
        snapshot_name=snapshot['name'],
        labels=labels,
    )
    gcloud = gcloud or ctx.gcloud()
    body = {
        'labels': labels,
        'labelFingerprint': snapshot['labelFingerprint'],
    }
    _log.debug(
        'snapshot.set-labels',
        key_hints=['body.labels'],
        body=body,
    )
    return await run_in_executor(
        gcloud.snapshots().setLabels(
            resource=snapshot['name'],
            project=ctx.config['gcloud_project'],
            body=body,
        ).execute
    )


def new_snapshot_name(ctx: Context, rule: Rule) -> str:
    """
    Get a new snapshot name for rule.
    Returns rule name and pendulum.now('utc') formatted according to settings.
    """

    time_str = re.sub(
        r'[^-a-z0-9]', '-',
        pendulum.now('utc').format(ctx.config['snapshot_datetime_format']),
        flags=re.IGNORECASE)

    # Won't be truncated
    suffix = f'-{time_str}'

    # Will be truncated
    name_truncated = rule.name[:63 - len(suffix)]

    return f'{name_truncated}{suffix}'


async def get_zone_operation(
        ctx: Context,
        zone: str,
        operation_name: str,
        *,
        gcloud: Optional[GoogleResource]=None
):
    gcloud = gcloud or ctx.gcloud()
    return await run_in_executor(
        gcloud.zoneOperations().get(
            project=ctx.config['gcloud_project'],
            zone=zone,
            operation=operation_name
        ).execute
    )


async def get_snapshot(
        ctx: Context,
        snapshot_name: str,
        *,
        gcloud: Optional[GoogleResource]=None
) -> Dict:
    gcloud = gcloud or ctx.gcloud()
    return await run_in_executor(
        gcloud.snapshots().get(
            snapshot=snapshot_name,
            project=ctx.config['gcloud_project']
        ).execute
    )


async def get_snapshots(ctx, reload_trigger):
    """Query the existing snapshots from Google Cloud.

    If the channel "reload_trigger" contains any value, we
    refresh the list of snapshots. This will then cause the
    next backup to be scheduled.
    """
    yield await load_snapshots(ctx)
    async for _ in reload_trigger:
        yield await load_snapshots(ctx)


async def load_snapshots(ctx) -> List[Dict]:
    resp = await run_in_executor(
        ctx.gcloud().snapshots()
        .list(
            project=ctx.config['gcloud_project'],
            filter=snapshot_list_filter(ctx),
        )
        .execute
    )
    return resp.get('items', [])


def snapshot_list_filter(ctx: Context) -> str:
    key, value = snapshot_author_label(ctx)
    return f'labels.{key} eq {value}'


def parse_creation_timestamp(snapshot: Dict) -> pendulum.Pendulum:
    return pendulum.parse(
        snapshot['creationTimestamp']
    ).in_timezone('utc')


def determine_next_snapshot(snapshots, rules):
    """
    Given a list of snapshots, and a list of rules, determine the next snapshot
    to be made.

    Returns a 2-tuple (rule, target_datetime)
    """
    next_rule = None
    next_timestamp = None

    for rule in rules:
        _log = _logger.new(rule=rule)
        # Find all the snapshots that match this rule
        snapshots_for_rule = filter_snapshots_by_rule(snapshots, rule)
        # Rewrite the list to snapshot
        snapshot_times = map(parse_creation_timestamp, snapshots_for_rule)
        # Sort by timestamp
        snapshot_times = sorted(snapshot_times, reverse=True)
        snapshot_times = list(snapshot_times)

        # There are no snapshots for this rule; create the first one.
        if not snapshot_times:
            next_rule = rule
            next_timestamp = pendulum.now('utc') + timedelta(seconds=10)
            _log.info(
                events.Snapshot.SCHEDULED,
                target=next_timestamp,
                key_hints=['rule.name', 'target'],
            )
            break

        target = snapshot_times[0] + rule.deltas[0]
        if not next_timestamp or target < next_timestamp:
            next_rule = rule
            next_timestamp = target

    if next_rule is not None and next_timestamp is not None:
        _logger.info(
            events.Snapshot.SCHEDULED,
            key_hints=['rule.name', 'target'],
            target=next_timestamp,
            rule=next_rule,
        )

    return next_rule, next_timestamp


def filter_snapshots_by_rule(snapshots, rule) -> Iterable:
    def match_disk(snapshot):
        url_part = '/zones/{zone}/disks/{name}'.format(
            zone=rule.gce_disk_zone, name=rule.gce_disk)
        return snapshot['sourceDisk'].endswith(url_part)
    return filter(match_disk, snapshots)
