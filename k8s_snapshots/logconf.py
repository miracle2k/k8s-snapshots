from collections import OrderedDict
from json import JSONEncoder

import logbook
import structlog
import sys

from datetime import timedelta
from structlog._frames import _find_first_app_frame_and_name


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

    def add_message(logger, method_name, event_dict):
        """
        Creates a ``message`` value based on the ``hint`` and ``key_hint`` keys.

        ``key_hint`` : ``Optional[str]``
            a '.'-separated path of dictionary keys.

        ``hint`` : ``Optional[str]``
            will be formatted using ``.format(**event_dict)``.
        """
        def from_key_hint(ed):
            key_hint = ed.get('key_hint')
            if key_hint is None:
                return

            value = ed

            for key in key_hint.split('.'):
                if value is None:
                    break
                value = value.get(key)

            return f'{key_hint}={value!r}'

        def from_hint(ed):
            hint = event_dict.get('hint')
            if hint is None:
                return

            try:
                return hint.format(**event_dict)
            except Exception as exc:
                return f'! error formatting message: {exc!r}'

        hints = [
            from_hint(event_dict),
            from_key_hint(event_dict)
        ]

        if all(hint is None for hint in hints):
            return event_dict

        prefix = event_dict['event']
        hint = ', '.join(hint for hint in hints if hint is not None)

        event_dict['message'] = f'{prefix}: {hint}'
        return event_dict

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

    key_order = ['event', 'level', 'message']

    if config['structlog_dev']:
        structlog.configure(
            processors=[
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
                add_severity,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt='ISO'),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                add_func_name,
                add_message,
                order_keys(key_order),
                structlog.processors.JSONRenderer(
                    indent=indent,
                    cls=TimeDeltaEncoder,
                )
            ],
            context_class=OrderedDict,
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=logger_factory,
            cache_logger_on_first_use=True,
        )


class TimeDeltaEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, timedelta):
            return str(timedelta)

        return super(TimeDeltaEncoder, self).default(o)
