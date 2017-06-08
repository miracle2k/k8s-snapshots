import structlog

DEFAULT_CONFIG = {
    'structlog_dev': False,
    'structlog_json_indent': 0,
    # Anything [^a-z0-9-] will be replaced by '-', the timezone will always be
    # UTC.
    'snapshot_datetime_format': '%Y-%m-%dT%H-%M-%S',
    'log_level': 'INFO',
    'gcloud_project': '',
    'gcloud_json_keyfile_name': '',
    'gcloud_json_keyfile_string': '',
    'kube_config_file': '',
    'use_claim_name': False,
    'deltas_annotation_key': 'backup.kubernetes.io/deltas'
}


def validate_config(config):
    required_keys = ('gcloud_project',)
    _logger = structlog.get_logger()

    result = True
    for key in required_keys:
        if not config.get(key):
            _logger.error(
                'Environment variable is required',
                key=key.upper())
            result = False

    return result
