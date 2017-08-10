from datetime import timedelta
import json
from typing import (
    TypeVar,
    Mapping,
    Sequence,
    Optional, Callable)

import isodate
import pendulum
from structlog.processors import _json_fallback_handler

Serializable = TypeVar(
    'Serializable',
    int,
    float,
    str,
    bool,
    Mapping,
    Sequence,
)

_DEFAULT_FALLBACK_PROCESSOR = _json_fallback_handler


def dumps(*args, **kwargs):
    kwargs['default'] = Processor()
    return json.dumps(*args, **kwargs)


class Processor:
    def __init__(self, fallback_processor=_DEFAULT_FALLBACK_PROCESSOR):
        self.fallback_processor = fallback_processor

    def __call__(self, obj):
        return process(obj, fallback_processor=self.fallback_processor)


def process(
        obj,
        fallback_processor: Optional[
            Callable[..., Serializable]
        ]=_DEFAULT_FALLBACK_PROCESSOR,
) -> Serializable:
    if isinstance(obj, timedelta):
        return isodate.duration_isoformat(obj)

    if isinstance(obj, pendulum.Pendulum):
        return obj.isoformat()

    if fallback_processor is not None:
        return fallback_processor(obj)

    raise TypeError(
        f'Cannot process object of type {type(obj)}, no fallback_processor '
        f'provided'
    )
