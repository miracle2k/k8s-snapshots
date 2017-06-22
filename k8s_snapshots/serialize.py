from datetime import timedelta
import json as _json
from typing import Union, Dict

import isodate
import pendulum

from k8s_snapshots.rule import Rule

#: Marker object used to differentiate between "nothing" and None
NOTHING = object()


class UnhandledTypeError(TypeError):
    pass


def dumps(*args, **kwargs):
    """
    Convenience wrapper around json.dumps that uses SnapshotsJSONEncoder
    by default.
    """
    kwargs.setdefault('cls', SnapshotsJSONEncoder)
    return _json.dumps(*args, **kwargs)


def to_str(value: Union[timedelta, pendulum.Pendulum]) -> str:
    if isinstance(value, timedelta):
        return serialize_duration(value)

    if isinstance(value, pendulum.Pendulum):
        return value.isoformat()

    raise UnhandledTypeError(
        f'Type {type(value)} can not be serialized to str()'
    )


def to_dict(value: Union[Rule]) -> Dict:
    if isinstance(value, Rule):
        return value.to_dict()

    raise UnhandledTypeError(
        f'Type {type(value)} can not be serialized to dict()'
    )


def serialize_duration(td: timedelta) -> str:
    return isodate.duration_isoformat(td)


class SnapshotsJSONEncoder(_json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        # We need to intercept the default=_json_fallback_encoder that
        # structlog.processors.JSONRenderer passes to json.dumps in order to
        # have our own .default()
        self._default_handler = kwargs.pop('default', None)
        super(SnapshotsJSONEncoder, self).__init__(*args, **kwargs)

    def default(self, obj):
        try:
            return to_str(obj)
        except UnhandledTypeError:
            pass

        try:
            return to_dict(obj)
        except UnhandledTypeError:
            pass

        if self._default_handler is not None:
            return self._default_handler(obj)
        else:
            return super(SnapshotsJSONEncoder, self).default(obj)
