from typing import Dict, List, NamedTuple
import digitalocean
from digitalocean.baseapi import NotFoundError
from .abstract import (
    DiskIdentifier, Snapshot, NewSnapshotIdentifier, SnapshotStatus)
import pendulum
import pykube.objects
from ..context import Context
import structlog


_logger = structlog.get_logger(__name__)


class DODiskIdentifier(NamedTuple):
    volume_id: str


class InvalidVolumeNameError(ValueError):
    def __init__(self, volume_name):
        super().__init__("DigitalOcean has no volume named %s.", volume_name)


def get_disk_identifier(
    volume: pykube.objects.PersistentVolume
) -> DODiskIdentifier:
    volume_id = volume.obj['spec']['csi']['volumeHandle']

    return DODiskIdentifier(volume_id=volume_id)


def supports_volume(volume: pykube.objects.PersistentVolume):
    csi = volume.obj['spec'].get('csi')
    return csi is not None and csi.get('driver') == 'dobs.csi.digitalocean.com'


def validate_disk_identifier(disk_id: Dict) -> DiskIdentifier:
    try:
        do_volumes = digitalocean.Manager().get_all_volumes()
        volume_name = disk_id['volumeName']
        do_volume = next((volume for volume in do_volumes
                          if volume.name == volume_name),
                         None)

        if do_volume is None:
            raise InvalidVolumeNameError(volume_name)

        return DODiskIdentifier(volume_id=do_volume.id)
    except InvalidVolumeNameError as err:
        raise err
    except:
        raise ValueError(disk_id)


def load_snapshots(
    ctx: Context, label_filters: Dict[str, str]
) -> List[Snapshot]:
    snapshots = digitalocean.Manager().get_volume_snapshots()

    tag_filters = set(k+':'+v for k, v in label_filters.items())
    filtered = [snapshot
                for snapshot in snapshots
                if tag_filters.intersection(snapshot.tags)]

    _logger.debug('digitalocean.load_snaphots', label_filters=label_filters,
                  tag_filters=tag_filters, snapshots_count=len(snapshots),
                  filtered=filtered)

    return list(map(lambda snapshot: Snapshot(
        name=snapshot.id,
        created_at=pendulum.parse(snapshot.created_at),
        disk=DODiskIdentifier(volume_id=snapshot.resource_id),
    ), filtered))


def create_snapshot(
    ctx: Context,
    disk: DODiskIdentifier,
    snapshot_name: str,
    snapshot_description: str
) -> NewSnapshotIdentifier:
    volume = digitalocean.Volume(id=disk.volume_id)

    snapshot = volume.snapshot(snapshot_name)

    return snapshot['snapshot']['id']


def get_snapshot_status(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier
) -> SnapshotStatus:
    # DO provides no way to know if a snapshost has finished
    return SnapshotStatus.COMPLETE


def set_snapshot_labels(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier,
    labels: Dict
):
    for label, value in labels.items():
        tag_name = label + ":" + value
        tag = digitalocean.Tag(name=tag_name)

        # Create the tag if it does not exist yet.
        _create_missing_tag(tag)

        tag.add_snapshots(snapshot_identifier)


def _create_missing_tag(tag: digitalocean.Tag):
    # If the tag does not exist, load() raise NotFoundError so we create it.
    try:
        tag.load()
        return
    except NotFoundError:
        tag.create()


def delete_snapshot(
    ctx: Context,
    snapshot: Snapshot
):
    do_snapshot = digitalocean.Manager().get_snapshot(snapshot.name)
    do_snapshot.destroy()
