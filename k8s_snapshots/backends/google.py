import json
import pendulum
from typing import List, Dict
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
import pykube.objects
import structlog
from k8s_snapshots.context import Context
from .abstract import Snapshot, SnapshotStatus, DiskIdentifier, NewSnapshotIdentifier
from ..errors import SnapshotCreateError


_logger = structlog.get_logger(__name__)



def validate_config(config):
    """Ensure the config of this backend is correct.
    """

    pass


def supports_volume(volume: pykube.objects.PersistentVolume):
    provisioner = volume.annotations.get('pv.kubernetes.io/provisioned-by')
    return provisioner == 'kubernetes.io/gce-pd'


def parse_timestamp(date_str: str) -> pendulum.Pendulum:
    return pendulum.parse(date_str).in_timezone('utc')


def snapshot_list_filter_expr(label_filters: Dict[str, str]) -> str:
    key = list(label_filters.keys())[0]
    value = label_filters[key]
    return f'labels.{key} eq {value}'


def load_snapshots(ctx, label_filters: Dict[str, str]) -> List[Snapshot]:
    """
    Return the existing snapshots.
    """
    resp = get_gcloud(ctx).snapshots().list(
        project=ctx.config['gcloud_project'],
        filter=snapshot_list_filter_expr(label_filters),
    ).execute()

    snapshots = []
    for item in resp.get('items', []):
        # We got to parse out the disk zone and name from the source disk.
        # It's an url that ends with '/zones/{zone}/disks/{name}'/
        _, zone, _, disk = item['sourceDisk'].split('/')[-4:]

        snapshots.append(Snapshot(
            name=item['name'],
            created_at=parse_timestamp(item['creationTimestamp']),
            disk=DiskIdentifier(zone_name=zone, disk_name=disk)
        ))

    return snapshots


def create_snapshot(
        ctx: Context,
        disk_name: str,
        disk_zone: str,
        snapshot_name: str,
        snapshot_description: str
) -> NewSnapshotIdentifier:
    request_body = {
        'name': snapshot_name,
        'description': snapshot_description
    }
    
    gcloud = get_gcloud(ctx)

    # Returns a ZoneOperation: {kind: 'compute#operation',
    # operationType: 'createSnapshot', ...}.
    # Google's documentation is confusing regarding this, since there's two
    # tables of payload parameter descriptions on the page, one of them
    # describes the input parameters, but contains output-only parameters,
    # the correct table can be found at
    # https://cloud.google.com/compute/docs/reference/latest/disks/createSnapshot#response
    operation = gcloud.disks().createSnapshot(
        disk=disk_name,
        project=ctx.config['gcloud_project'],
        zone=disk_zone,
        body=request_body
    ).execute()

    return {
        'snapshot_name': snapshot_name,
        'zone': disk_zone,
        'operation_name': operation['name']
    }


def get_snapshot_status(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier
) -> SnapshotStatus:
    """In Google Cloud, the createSnapshot operation returns a ZoneOperation
    object which goes from PENDING, to RUNNING, to DONE.
    The snapshot object itself can be CREATING, DELETING, FAILED, READY,
    or UPLOADING.

    We check both states to make sure the snapshot was created.
    """

    _log = _logger.new(
        snapshot_identifier=snapshot_identifier,
    )
    
    gcloud = get_gcloud(ctx)
    
    # First, check the operation state
    operation = gcloud.zoneOperations().get(
        project=ctx.config['gcloud_project'],
        zone=snapshot_identifier['zone'],
        operation=snapshot_identifier['operation_name']
    ).execute()

    if not operation['status'] == 'DONE':
        _log.debug('google.status.operation_not_complete',
                   status=operation['status'])
        return SnapshotStatus.PENDING

    # To be sure, check the state of the snapshot itself
    snapshot = gcloud.snapshots().get(
        snapshot=snapshot_identifier['snapshot_name'],
        project=ctx.config['gcloud_project']
    ).execute()

    status = snapshot['status']
    if status == 'FAILED':
        _log.debug('google.status.failed',
                   status=status)
        raise SnapshotCreateError(status)
    elif status != 'READY':
        _log.debug('google.status.not_ready',
                   status=status)
        return SnapshotStatus.PENDING

    return SnapshotStatus.COMPLETE


def set_snapshot_labels(
    ctx: Context,
    snapshot_identifier: NewSnapshotIdentifier,
    labels: Dict
):
    gcloud = get_gcloud(ctx)

    snapshot = gcloud.snapshots().get(
        snapshot=snapshot_identifier['snapshot_name'],
        project=ctx.config['gcloud_project']
    ).execute()
    
    body = {
        'labels': labels,
        'labelFingerprint': snapshot['labelFingerprint'],
    }
    return gcloud.snapshots().setLabels(
        resource=snapshot_identifier['snapshot_name'],
        project=ctx.config['gcloud_project'],
        body=body,
    ).execute()


def delete_snapshot(
    ctx: Context,
    snapshot: Snapshot
):
    gcloud = get_gcloud(ctx)
    return gcloud.snapshots().delete(
        snapshot=snapshot.name,
        project=ctx.config['gcloud_project']
    ).execute()


def get_gcloud(ctx, version: str= 'v1'):
    """
    Get a configured Google Compute API Client instance.

    Note that the Google API Client is not threadsafe. Cache the instance locally
    if you want to avoid OAuth overhead between calls.

    Parameters
    ----------
    version
        Compute API version
    """
    SCOPES = 'https://www.googleapis.com/auth/compute'
    credentials = None

    if ctx.config.get('gcloud_json_keyfile_name'):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            ctx.config.get('gcloud_json_keyfile_name'),
            scopes=SCOPES)

    if ctx.config.get('gcloud_json_keyfile_string'):
        keyfile = json.loads(ctx.config.get('gcloud_json_keyfile_string'))
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            keyfile, scopes=SCOPES)

    if not credentials:
        credentials = GoogleCredentials.get_application_default()

    if not credentials:
        raise RuntimeError("Auth for Google Cloud was not configured")

    compute = discovery.build(
        'compute',
        version,
        credentials=credentials,
        # https://github.com/google/google-api-python-client/issues/299#issuecomment-268915510
        cache_discovery=False
    )
    return compute
