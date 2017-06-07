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
                structlog.dev.ConsoleRenderer()  # <===
            ],
            context_class=dict,
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
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.TimeStamper(fmt='ISO'),
                add_func_name,
                structlog.processors.JSONRenderer(
                    indent=indent,
                    sort_keys=True,
                    cls=TimeDeltaEncoder,
                )
            ],
            # context_class=dict,
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=logger_factory,
            cache_logger_on_first_use=True,
        )


class TimeDeltaEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, timedelta):
            return str(timedelta)

        return super(TimeDeltaEncoder, self).default(o)
