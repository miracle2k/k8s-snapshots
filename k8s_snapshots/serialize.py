from datetime import timedelta
import json as _json

import isodate
import pendulum

from k8s_snapshots.rule import Rule


def dumps(*args, **kwargs):
    """
    Convenience wrapper around json.dumps that uses SnapshotsJSONEncoder
    by default.
    """
    kwargs.setdefault('cls', SnapshotsJSONEncoder)
    return _json.dumps(*args, **kwargs)


class SnapshotsJSONEncoder(_json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        # We need to intercept the default=_json_fallback_encoder that
        # structlog.processors.JSONRenderer passes to json.dumps in order to
        # have our own .default()
        self._default_handler = kwargs.pop('default', None)
        super(SnapshotsJSONEncoder, self).__init__(*args, **kwargs)

    def default(self, obj):
        if isinstance(obj, timedelta):
            return isodate.duration_isoformat(obj)

        if isinstance(obj, pendulum.Pendulum):
            return obj.isoformat()

        if isinstance(obj, Rule):
            return obj.to_dict()

        if self._default_handler is not None:
            return self._default_handler(obj)
        else:
            return super(SnapshotsJSONEncoder, self).default(obj)
