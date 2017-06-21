import pendulum
import re
import structlog

DEFAULT_CONFIG = {
    #: Set to True to make logs more human-readable
    'structlog_dev': False,
    #: If zero, prints one line of JSON per message, if set to a positive
    #: non-zero integer to get indented JSON output
    'structlog_json_indent': 0,
    #: Anything [^a-z0-9-] will be replaced by '-', the timezone will always be
    #: UTC.
    'snapshot_datetime_format': '%Y-%m-%dt%H-%M-%Sz',
    'log_level': 'INFO',
    'gcloud_project': '',
    'gcloud_json_keyfile_name': '',
    'gcloud_json_keyfile_string': '',
    'kube_config_file': '',
    'use_claim_name': False,
    #: The key used when annotating PVs and PVCs with deltas
    'deltas_annotation_key': 'backup.kubernetes.io/deltas',
    #: The label key on a GCE Snapshot where the Rule for which a snapshot was
    #: created is stored.
    'snapshot_label_rule': 'backup.kubernetes.io/rule',
    #: Turns debug mode on, not recommended in production
    'debug': False,
}

#: The regex that a snapshot name has to match.
#: Regex provided by the createSnapshot error response.
GOOGLE_SNAPSHOT_NAME_REGEX = r'^(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)$'


def validate_config(config):
    required_keys = {'gcloud_project'}
    _logger = structlog.get_logger()

    missing_required_keys = required_keys - set(config.keys())

    if missing_required_keys:
        _logger.error(
            'config.error',
            missing_required_keys=missing_required_keys
        )
        return False

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
        return False

    return True
