import logging
import logging.config
from collections import OrderedDict
from typing import Optional, List, Any, Dict

import structlog
import sys

from k8s_snapshots import serialize


class ProcessStructuredErrors:
    def __init__(self):
        pass

    def __call__(self, logger, method_name, event_dict):
        exc_info = event_dict.pop('exc_info', None)

        if exc_info is None:
            return event_dict

        exc_type, exc, exc_tb = structlog.processors._figure_out_exc_info(
            exc_info)

        __structlog__ = getattr(exc, '__structlog__', None)

        if not callable(__structlog__):
            event_dict['exc_info'] = exc_info
            return event_dict

        structured_error = __structlog__()
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

            __structlog__ = getattr(value, '__structlog__', None)
            if __structlog__ is not None:
                value = __structlog__()

            value = value.get(key)

        return value

    def from_key_hint(ed) -> Optional[str]:
        key_hint = ed.pop('key_hint', None)
        if key_hint is None:
            return

        value = path_value(ed, key_hint)

        return format_kv(key_hint, value)

    def from_key_hints(ed) -> List[str]:
        key_hints = ed.pop('key_hints', None)
        if key_hints is None:
            return []

        return [
            format_kv(key_hint, path_value(ed, key_hint))
            for key_hint in key_hints
        ]

    def format_kv(key: str, value: Any) -> str:
        return f'{key}={serialize.process(value)}'

    hints = [
        from_hint(event_dict),
        from_key_hint(event_dict)
    ]
    hints += from_key_hints(event_dict)

    if all(hint is None for hint in hints):
        if event_dict.get('message') is None:
            event_dict['message'] = event_dict.get('event')
        return event_dict

    prefix = event_dict['event']
    hint = ', '.join(hint for hint in hints if hint is not None)

    message = event_dict.get('message')
    if message is not None:
        message = f'{prefix}: {message}, {hint}'
    else:
        message = f'{prefix}: {hint}'

    event_dict['message'] = message
    return event_dict


def configure_from_config(config):
    configure_logging(
        level_name=config['log_level'],
        for_humans=not config['json_log'],
        json_indent=config['structlog_json_indent'] or None,
    )


def configure_logging(
        level_name: str='INFO',
        for_humans: bool=False,
        json_indent: Optional[int]=None,
):
    configure_structlog(
        for_humans=for_humans,
        json_indent=json_indent,
        level_name=level_name,
    )


def configure_structlog(
        for_humans: bool=False,
        json_indent: Optional[int]=None,
        level_name: str='INFO'
):
    key_order = ['message', 'event', 'level']
    timestamper = structlog.processors.TimeStamper(fmt='ISO')

    processors = [
        event_enum_to_str,
        ProcessStructuredErrors(),
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        rename_level_to_severity,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        add_func_name,
        add_message,
        #order_keys(key_order),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    if for_humans:
        renderer = structlog.dev.ConsoleRenderer()  # <===
    else:
        # Make it so that 0 ⇒ None
        indent = json_indent or None
        renderer = structlog.processors.JSONRenderer(
            indent=indent,
            serializer=serialize.dumps
        )

    foreign_pre_chain = [
        # Add the log level and a timestamp to the event_dict if the log entry
        # is not from structlog.
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        foreign_event_to_message,
        rename_level_to_severity,
        timestamper,
    ]

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'structlog': {
                '()': structlog.stdlib.ProcessorFormatter,
                'processor': renderer,
                'foreign_pre_chain': foreign_pre_chain,
            },
        },
        'handlers': {
            'default': {
                'level': level_name,
                'class': 'logging.StreamHandler',
                'stream': sys.stdout,
                'formatter': 'structlog',
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'DEBUG',
                'propagate': True,
            },
        }
    })

    structlog.configure(
        processors=processors,
        context_class=OrderedDict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def foreign_event_to_message(logger, method_name, event_dict):
    event = event_dict.get('event')

    if event is not None and 'message' not in event_dict:
        event_dict['message'] = event
        event_dict['event'] = 'foreign'

    return event_dict


def rename_level_to_severity(logger, method_name, event_dict):
    level = event_dict.pop('level', None)

    event_dict['severity'] = level.upper()

    return event_dict


def add_func_name(logger, method_rame, event_dict):
    record = event_dict.get('_record')
    if record is None:
        return event_dict

    event_dict['function'] = record.funcName

    return event_dict


def order_keys(order):
    """
    Order keys for JSON readability when not using json_log=True
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
        event_dict['event'] = event.value

    return event_dict
