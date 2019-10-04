import os
from typing import Dict
import confcollect
import structlog
from k8s_snapshots.errors import ConfigurationError


_logger = structlog.get_logger()


DEFAULT_CONFIG = {
    #: Set to True to make logs more machine-readable
    'json_log': False,
    #: If zero, prints one line of JSON per message, if set to a positive
    #: non-zero integer to get indented JSON output
    'structlog_json_indent': 0,
    #: Anything [^a-z0-9-] will be replaced by '-', the timezone will always be
    #: UTC.
    'snapshot_datetime_format': '%d%m%y-%H%M%S',
    'log_level': 'INFO',
    'kube_config_file': '',
    'use_claim_name': False,
    'ping_url': '',
    #: The key used when annotating PVs and PVCs with deltas
    'deltas_annotation_key': 'backup.kubernetes.io/deltas',
    #: This label will be set on all snapshots created by k8s-snapshots
    'snapshot_author_label': 'k8s-snapshots',
    'snapshot_author_label_key': 'created-by',
    #: Number of seconds between Rule.HEARTBEAT events, ``0`` to disable.
    'schedule_heartbeat_interval_seconds': 600,
    #: Turns debug mode on, not recommended in production
    'debug': False,

    'gcloud_project': '',
    'gcloud_credentials_file': os.path.join(
        os.path.expanduser('~'),
        ".config/gcloud/application_default_credentials.json"
    ),
    'google_application_credentials': '',

    'aws_region': ''
}


def validate_config(config: Dict) -> bool:
    return True


def from_environ_basic() -> Dict:
    config = DEFAULT_CONFIG.copy()
    config.update(confcollect.from_environ(by_defaults=DEFAULT_CONFIG))
    # Backwards compatability
    if config.get('gcloud_json_keyfile_name') and not config.get('gcloud_credentials_file'):
        config['gcloud_credentials_file'] = config.get('gcloud_json_keyfile_name')
    if config.get('gcloud_json_keyfile_string') and not config.get('google_application_credentials'):
        config['google_application_credentials'] = config.get('gcloud_json_keyfile_string')

    return config


def from_environ() -> Dict:
    config = from_environ_basic()

    if not validate_config(config):
        raise ConfigurationError(
            'Invalid configuration. See log for more details',
            config=config
        )

    return config
