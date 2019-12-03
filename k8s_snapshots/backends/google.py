import json
import pendulum
import re
import requests
from typing import List, Dict, NamedTuple
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
import pykube.objects
import structlog
from k8s_snapshots.context import Context
from .abstract import Snapshot, SnapshotStatus, DiskIdentifier, NewSnapshotIdentifier
from ..errors import SnapshotCreateError, UnsupportedVolume


_logger = structlog.get_logger(__name__)


#: The regex that a snapshot name has to match.
#: Regex provided by the createSnapshot error response.
GOOGLE_SNAPSHOT_NAME_REGEX = r'^(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)$'

# Google Label keys and values must conform to the following restrictions:
# - Keys and values cannot be longer than 63 characters each.
# - Keys and values can only contain lowercase letters, numeric characters,
#   underscores, and dashes. International characters are allowed.
# - Label keys must start with a lowercase letter and international characters
#   are allowed.
# - Label keys cannot be empty.
# See https://cloud.google.com/compute/docs/labeling-resources for more

#: The regex that a label key and value has to match, additionally it has to be
#: lowercase, this is checked with str().islower()
GOOGLE_LABEL_REGEX = r'^(?:[-\w]{0,63})$'


def get_project_id(ctx: Context):
    if not ctx.config['gcloud_project']:
        response = requests.get(
            'http://metadata.google.internal/computeMetadata/v1/project/project-id',
            headers={
                'Metadata-Flavor': 'Google'
            })
        response.raise_for_status()
        ctx.config['gcloud_project'] = response.text

    return ctx.config['gcloud_project']


# TODO: This is currently not called. When should we do so? Once the Google
# Cloud backend is loaded for the first time?
def validate_config(config):
    """Ensure the config of this backend is correct.
    """

    is_valid = True

    test_datetime = pendulum.now('utc').format(
        config['snapshot_datetime_format'])
    test_snapshot_name = f'dummy-snapshot-{test_datetime}'

    if not re.match(GOOGLE_SNAPSHOT_NAME_REGEX, test_snapshot_name):
        _logger.error(
            'config.error',
            key='snapshot_datetime_format',
            message='Snapshot datetime format returns invalid string. '
                    'Note that uppercase characters are forbidden.',
            test_snapshot_name=test_snapshot_name,
            regex=GOOGLE_SNAPSHOT_NAME_REGEX
        )
        is_valid = False

    # Configuration keys that are either a Google
    glabel_key_keys = {'snapshot_author_label'}
    glabel_value_keys = {'snapshot_author_label_key'}

    for key in glabel_key_keys | glabel_value_keys:
        value = config[key]  # type: str
        re_match = re.match(GOOGLE_LABEL_REGEX, value)
        is_glabel_key = key in glabel_key_keys
        is_glabel_valid = (
            re_match and value.islower() and
            value[0].isalpha() or not is_glabel_key
        )

        if not is_glabel_valid:
            _logger.error(
                'config.error',
                message=f'Configuration value is not a valid '
                        f'Google Label {"Key" if is_glabel_key else "Value"}. '
                        f'See '
                        f'https://cloud.google.com/compute/docs/labeling-resources '
                        f'for more',
                key_hints=['value', 'regex'],
                key=key,
                is_lower=value.islower(),
                value=config[key],
                regex=GOOGLE_LABEL_REGEX,
            )
            is_valid = False

    return is_valid


class GoogleDiskIdentifier(NamedTuple):
    name: str
    zone: str
    region: str
    regional: bool


def get_disk_identifier(volume: pykube.objects.PersistentVolume) -> GoogleDiskIdentifier:
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
    gce_disk_zone = volume.labels.get(
        'failure-domain.beta.kubernetes.io/zone'
    )

    if not gce_disk_zone:
        raise UnsupportedVolume('cannot find the zone of the disk')

    gce_disk_region = volume.labels.get(
        'failure-domain.beta.kubernetes.io/region'
    )

    if not gce_disk_region:
        raise UnsupportedVolume('cannot find the region of the disk')

    if "__" in gce_disk_zone:
        # seems like Google likes to put __ in between zones in the label
        # failure-domain.beta.kubernetes.io/zone when the pv is regional
        return GoogleDiskIdentifier(name=gce_disk, zone="N/A", region=gce_disk_region, regional=True)
    else:
        return GoogleDiskIdentifier(name=gce_disk, zone=gce_disk_zone, region="N/A", regional=False)


def supports_volume(volume: pykube.objects.PersistentVolume):
    return bool(volume.obj['spec'].get('gcePersistentDisk'))


def parse_timestamp(date_str: str) -> pendulum.Pendulum:
    return pendulum.parse(date_str).in_timezone('utc')


def validate_disk_identifier(disk_id: Dict) -> DiskIdentifier:
    """Should take the user-specified dictionary, and convert it to
    it's own, local `DiskIdentifier`. If the disk_id is not valid,
    it should raise a `ValueError` with a suitable error message.
    """

    try:
        return GoogleDiskIdentifier(
            zone=disk_id['zone'],
            name=disk_id['name']
        )
    except:
        raise ValueError(disk_id)


def snapshot_list_filter_expr(label_filters: Dict[str, str]) -> str:
    key = list(label_filters.keys())[0]
    value = label_filters[key]
    return f'labels.{key} eq {value}'


def load_snapshots(ctx, label_filters: Dict[str, str]) -> List[Snapshot]:
    """
    Return the existing snapshots.
    """
    snapshots = get_gcloud(ctx).snapshots()
    request = snapshots.list(
        project=get_project_id(ctx),
        filter=snapshot_list_filter_expr(label_filters),
        maxResults=500,
    )

    loaded_snapshots = []

    while request is not None:
        resp = request.execute()
        for item in resp.get('items', []):
            # We got to parse out the disk zone and name from the source disk.
            # It's an url that ends with '/zones/{zone}/disks/{name}'/
            sourceDiskList = item['sourceDisk'].split('/')

            disk = sourceDiskList[-1]

            if "regions" in sourceDiskList:
                region = sourceDiskList[8]
                loaded_snapshots.append(Snapshot(
                    name=item['name'],
                    created_at=parse_timestamp(item['creationTimestamp']),
                    disk=GoogleDiskIdentifier(name=disk, region=region, zone="N/A", regional=True)
                ))
            else:
                zone = sourceDiskList[8]
                loaded_snapshots.append(Snapshot(
                    name=item['name'],
                    created_at=parse_timestamp(item['creationTimestamp']),
                    disk=GoogleDiskIdentifier(name=disk, region="N/A", zone=zone, regional=False)
                ))

        request = snapshots.list_next(request, resp)

    return loaded_snapshots


def create_snapshot(
    ctx: Context,
    disk: GoogleDiskIdentifier,
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
    if disk.regional:
        operation = gcloud.regionDisks().createSnapshot(
            disk=disk.name,
            project=get_project_id(ctx),
            region=disk.region,
            body=request_body
        ).execute()
        return {
            'snapshot_name': snapshot_name,
            'region': disk.region,
            'operation_name': operation['name']
        }

    else:
        operation = gcloud.disks().createSnapshot(
            disk=disk.name,
            project=get_project_id(ctx),
            zone=disk.zone,
            body=request_body
        ).execute()
        return {
            'snapshot_name': snapshot_name,
            'zone': disk.zone,
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

    if "region" in snapshot_identifier:
        operation = gcloud.regionOperations().get(
            project=get_project_id(ctx),
            region=snapshot_identifier['region'],
            operation=snapshot_identifier['operation_name']
        ).execute()
    else:
        operation = gcloud.zoneOperations().get(
            project=get_project_id(ctx),
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
        project=get_project_id(ctx)
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
        project=get_project_id(ctx)
    ).execute()

    body = {
        'labels': labels,
        'labelFingerprint': snapshot['labelFingerprint'],
    }
    return gcloud.snapshots().setLabels(
        resource=snapshot_identifier['snapshot_name'],
        project=get_project_id(ctx),
        body=body,
    ).execute()


def delete_snapshot(
    ctx: Context,
    snapshot: Snapshot
):
    gcloud = get_gcloud(ctx)
    return gcloud.snapshots().delete(
        snapshot=snapshot.name,
        project=get_project_id(ctx)
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

    if ctx.config.get('gcloud_credentials_file'):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            ctx.config.get('gcloud_credentials_file'),
            scopes=SCOPES)

    if ctx.config.get('google_application_credentials'):
        keyfile = json.loads(ctx.config.get('google_application_credentials'))
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
