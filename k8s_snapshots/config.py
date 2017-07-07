import os
from typing import Dict

import confcollect
import pendulum
import re
import structlog
from tarsnapper.config import ConfigError, parse_deltas

from k8s_snapshots import events
from k8s_snapshots.errors import ConfigurationError
from k8s_snapshots.rule import Rule

_logger = structlog.get_logger()


DEFAULT_CONFIG = {
    #: Set to True to make logs more human-readable
    'structlog_dev': False,
    #: If zero, prints one line of JSON per message, if set to a positive
    #: non-zero integer to get indented JSON output
    'structlog_json_indent': 0,
    #: Anything [^a-z0-9-] will be replaced by '-', the timezone will always be
    #: UTC.
    'snapshot_datetime_format': '%d%m%y-%H%M%S',
    'log_level': 'INFO',
    'gcloud_project': '',
    'gcloud_json_keyfile_name': '',
    'gcloud_json_keyfile_string': '',
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
}

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


def validate_config(config: Dict) -> bool:
    required_keys = {'gcloud_project'}

    is_valid = True

    missing_required_keys = required_keys - \
        set({k: v for k, v in config.items() if v}.keys())

    if missing_required_keys:
        _logger.error(
            'config.error',
            missing_required_keys=missing_required_keys
        )
        is_valid = False

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


def from_environ() -> Dict:
    config = DEFAULT_CONFIG.copy()
    config.update(confcollect.from_environ(by_defaults=DEFAULT_CONFIG))

    # Read manual volume definitions
    try:
        config.update(read_volume_config())
    except ValueError as exc:
        _logger.exception('config.error')
        raise ConfigurationError(
            'Could not read volume configuration',
            config=config,
        ) from exc

    if not validate_config(config):
        raise ConfigurationError(
            'Invalid configuration. See log for more details',
            config=config
        )

    return config


def read_volume_config() -> Dict:
    """Read the volume configuration from the environment
    """
    def read_volume(name):
        _log = _logger.new(
            volume_name=name,
        )
        env_name = name.replace('-', '_').upper()
        deltas_str = os.environ.get('VOLUME_{}_DELTAS'.format(env_name))
        if not deltas_str:
            raise ConfigError('A volume {} was defined, but VOLUME_{}_DELTAS is not set'.format(name, env_name))

        zone = os.environ.get('VOLUME_{}_ZONE'.format(env_name))
        if not zone:
            raise ConfigError('A volume {} was defined, but VOLUME_{}_ZONE is not set'.format(name, env_name))

        _log = _log.bind(
            deltas_str=deltas_str,
            zone=zone,
        )

        rule = Rule(
            name=name,
            claim_name='',
            namespace='',
            deltas=parse_deltas(deltas_str),
            gce_disk=name,
            gce_disk_zone=zone,
        )

        _log.info(events.Rule.ADDED_FROM_CONFIG, rule=rule)

        return rule

    volumes = filter(bool, map(lambda s: s.strip(), os.environ.get('VOLUMES', '').split(',')))
    config = {}
    config['rules'] = list(filter(bool, map(read_volume, volumes)))
    return config
