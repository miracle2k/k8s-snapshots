from typing import Dict, List, NamedTuple
import pykube.objects
import requests
import pendulum
from urllib.parse import urlparse
from boto import ec2
from ..context import Context
from k8s_snapshots.snapshot import Snapshot
from .abstract import NewSnapshotIdentifier, SnapshotStatus
from ..errors import SnapshotCreateError


def validate_config(config):
    """Ensure the config of this backend is correct.

    manual volumes are validated by the backend
        - for aws, google cloud, need different data, say, region or zone.
    """
    pass


def supports_volume(volume: pykube.objects.PersistentVolume):
    return bool(volume.obj['spec'].get('awsElasticBlockStore'))


class AWSDiskIdentifier(NamedTuple):
    region: str
    volume_id: str


def get_current_region(ctx):
    """Get the current region from the metadata service.
    """
    if not ctx.config['aws_region']:
        response = requests.get(
            'http://169.254.169.254/latest/meta-data/placement/availability-zone',
            timeout=5)
        response.raise_for_status()
        ctx.config['aws_region'] = response.text[:-1]

    return ctx.config['aws_region']



def get_disk_identifier(volume: pykube.objects.PersistentVolume):
    # An url such as aws://eu-west-1a/vol-00292b2da3d4ed1e4
    volume_url = volume.obj['spec'].get('awsElasticBlockStore')['volumeID']

    parts = urlparse(volume_url)
    zone = parts.netloc
    volume_id = parts.path[1:]

    return AWSDiskIdentifier(region=zone[:-1], volume_id=volume_id)


def parse_timestamp(date_str: str) -> pendulum.Pendulum:
    return pendulum.parse(date_str).in_timezone('utc')


def validate_disk_identifier(disk_id: Dict):
    try:
        return AWSDiskIdentifier(
            region=disk_id['region'],
            volume_id=disk_id['volumeId']
        )
    except:
        raise ValueError(disk_id)

# AWS can filter by volume-id, which means we wouldn't have to match in Python.
# In any case, it might be easier to let the backend handle the matching. Then
# it relies less on the DiskIdentifier object always matching.
#filters={'volume-id': volume.id}
def load_snapshots(ctx: Context, label_filters: Dict[str, str]) -> List[Snapshot]:
    connection = get_connection(ctx, region=get_current_region(ctx))

    snapshots = connection.get_all_snapshots(
        owner='self',
        filters={f'tag:{k}': v for k, v in label_filters.items()}
    )

    return list(map(lambda snapshot: Snapshot(
        name=snapshot.id,
        created_at=parse_timestamp(snapshot.start_time),
        disk=AWSDiskIdentifier(
            volume_id=snapshot.volume_id,
            region=snapshot.region.name
        )
    ), snapshots))


def create_snapshot(
    ctx: Context,
    disk: AWSDiskIdentifier,
    snapshot_name: str,
    snapshot_description: str
) -> NewSnapshotIdentifier:

    connection = get_connection(ctx, disk.region)

    # TODO: Seems like the API doesn't actually allow us to set a snapshot
    # name, although it's possible in the UI.
    snapshot = connection.create_snapshot(
        disk.volume_id,
        description=snapshot_name
    )
    
    return {
        'id': snapshot.id,
        'region': snapshot.region.name
    }


def get_snapshot_status(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier
) -> SnapshotStatus:
    connection = get_connection(ctx, snapshot_identifier['region'])

    snapshots = connection.get_all_snapshots(
        [snapshot_identifier['id']]
    )
    snapshot = snapshots[0]
    
    # Can be pending | completed | error
    if snapshot.status == 'pending':
        return SnapshotStatus.PENDING
    elif snapshot.status == 'completed':
        return SnapshotStatus.COMPLETE
    elif snapshot.status == 'error':
        raise SnapshotCreateError(snapshot['status'])
    else:
        raise NotImplementedError()


def set_snapshot_labels(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier,
    labels: Dict
):
    connection = get_connection(ctx, snapshot_identifier['region'])
    connection.create_tags([snapshot_identifier['id']], labels)


def delete_snapshot(
    ctx: Context,
    snapshot: Snapshot
):
    connection = get_connection(ctx, snapshot.disk.region)
    connection.delete_snapshot(snapshot.name)


def get_connection(ctx: Context, region):
    connection = ec2.connect_to_region(region)
    return connection
