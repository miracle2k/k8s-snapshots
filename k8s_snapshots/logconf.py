import traceback
from collections import OrderedDict
from json import JSONEncoder

import pendulum
from typing import Optional, List, Any, Dict, Iterable

import logbook
import structlog

from datetime import timedelta
from structlog._frames import _find_first_app_frame_and_name

from k8s_snapshots.errors import StructuredError


class ProcessStructuredErrors:
    def __init__(self):
        pass

    def _exc_chain(self, start_exc: Exception) -> Iterable[Exception]:
        chain = []  # reverse chronological order
        exc = start_exc

        while exc is not None:
            chain.append(exc)
            exc = exc.__cause__

        return reversed(chain)

    def _serializable_exc(self, exc_: Exception) -> List[Dict]:
        def serialize_exc(exc: Exception) -> Dict:
            if isinstance(exc, StructuredError):
                return exc.to_dict()
            else:
                exc_type = exc.__class__
                exc_tb = exc.__traceback__
                return {
                    'type': exc_type.__qualname__,
                    'message': str(exc),
                    'readable': traceback.format_exception(
                        exc_type,
                        exc,
                        exc_tb,
                        chain=False
                    )
                }

        return [serialize_exc(exc) for exc in self._exc_chain(exc_)]

    def __call__(self, logger, method_name, event_dict):
        exc_info = event_dict.pop('exc_info', None)

        if exc_info is None:
            return event_dict

        exc_type, exc, exc_tb = structlog.processors._figure_out_exc_info(
            exc_info)

        if not isinstance(exc, StructuredError):
            event_dict['exc_info'] = exc_info
            return event_dict

        structured_error = self._serializable_exc(exc)
        event_dict['structured_error'] = structured_error

        return event_dict


def add_message(logger, method_name, event_dict):
    """
    Creates a ``message`` value based on the ``hint`` and ``key_hint`` keys.

    ``key_hint`` : ``Optional[str]``
        a '.'-separated path of dictionary keys.

    ``hint`` : ``Optional[str]``
        will be formatted using ``.format(**event_dict)``.
    """
    def from_hint(ed):
        hint = event_dict.pop('hint', None)
        if hint is None:
            return

        try:
            return hint.format(**event_dict)
        except Exception as exc:
            return f'! error formatting message: {exc!r}'

    def path_value(dict_: Dict[str, Any], key_path: str) -> Optional[Any]:
        value = dict_

        for key in key_path.split('.'):
            if value is None:
                return
            value = value.get(key)

        return value

    def from_key_hint(ed) -> Optional[str]:
        key_hint = ed.pop('key_hint', None)
        if key_hint is None:
            return

        value = path_value(ed, key_hint)

        return f'{key_hint}={value!r}'

    def from_key_hints(ed) -> List[str]:
        key_hints = ed.pop('key_hints', None)
        if key_hints is None:
            return []

        return [
            f'{key_hint}={path_value(ed, key_hint)}'
            for key_hint in key_hints
        ]

    hints = [
        from_hint(event_dict),
        from_key_hint(event_dict)
    ]
    hints += from_key_hints(event_dict)

    if all(hint is None for hint in hints):
        return event_dict

    prefix = event_dict['event']
    hint = ', '.join(hint for hint in hints if hint is not None)

    event_dict['message'] = f'{prefix}: {hint}'
    return event_dict


def serialize_rules(logger, method_name, event_dict):
    """
    Replace Rule instances with their .to_dict() representation in time for
    add_message to use attributes of it via key_hints.
    """
    from k8s_snapshots.core import Rule

    updates = {}
    for key, value in event_dict.items():
        if isinstance(value, Rule):
            updates[key] = value.to_dict()

    event_dict.update(updates)
    return event_dict


def configure_logging(config):
    level = logbook.lookup_level(config['log_level'])
    handler = logbook.StderrHandler(
        level=level,
        format_string='{record.message}')

    handler.push_application()

    def logger_factory(name=None):
        if name is None:
            _, name = _find_first_app_frame_and_name(
                additional_ignores=[
                    f'{__package__}.logconf',
                ]
            )
        return logbook.Logger(name, level=level)

    def add_severity(logger, method_name, event_dict):
        if method_name == 'warn':
            method_name = 'warning'

        event_dict['severity'] = method_name.upper()
        return event_dict

    def add_func_name(logger, method_rame, event_dict):
        record = event_dict.get('_record')
        if record is None:
            return event_dict

        event_dict['function'] = record.funcName

        return event_dict

    def order_keys(order):
        """
        Order keys for JSON readability when not using structlog_dev=True
        """
        def processor(logger, method_name, event_dict):
            if not isinstance(event_dict, OrderedDict):
                return event_dict

            for key in reversed(order):
                if key in event_dict:
                    event_dict.move_to_end(key, last=False)

            return event_dict
        return processor

    def event_enum_to_str(logger, method_name, event_dict):
        from k8s_snapshots import events
        event = event_dict.get('event')
        if event is None:
            return event_dict

        if isinstance(event, events.EventEnum):
            event_dict['snapshot_event'] = event
            event_dict['event'] = event.value

        return event_dict

    key_order = ['message', 'event', 'level']

    if config['structlog_dev']:
        structlog.configure(
            processors=[
                event_enum_to_str,
                ProcessStructuredErrors(),
                serialize_rules,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt='ISO'),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                add_func_name,
                add_message,
                order_keys(key_order),
                structlog.dev.ConsoleRenderer()  # <===
            ],
            context_class=OrderedDict,
            logger_factory=logger_factory,
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    else:
        # Make it so that 0 ⇒ None
        indent = config['structlog_json_indent'] or None
        structlog.configure(
            processors=[
                event_enum_to_str,
                add_severity,
                ProcessStructuredErrors(),
                serialize_rules,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt='ISO'),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                add_func_name,
                add_message,
                order_keys(key_order),
                structlog.processors.JSONRenderer(
                    indent=indent,
                    cls=SnapshotsJSONEncoder,
                )
            ],
            context_class=OrderedDict,
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=logger_factory,
            cache_logger_on_first_use=True,
        )


class SnapshotsJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, timedelta):
            return str(timedelta)

        if isinstance(o, pendulum.Pendulum):
            return o.isoformat()

        return super(SnapshotsJSONEncoder, self).default(o)
