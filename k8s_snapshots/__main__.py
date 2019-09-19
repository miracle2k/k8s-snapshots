import asyncio
import functools
import signal
import sys

import confcollect
import structlog

import k8s_snapshots.config
from k8s_snapshots.logconf import configure_from_config
from k8s_snapshots.core import daemon


def main():
    # Read config initially just to setup logging
    configure_from_config(k8s_snapshots.config.from_environ_basic())

    # Now with logging setup, read and validate the config.
    config = k8s_snapshots.config.from_environ()
    
    if config['debug']:
        sys.excepthook = debug_excepthook

    # Late import to keep module-level get_logger after configure_from_config
    _logger = structlog.get_logger(__name__)

    _logger.bind(
        gcloud_project=config['gcloud_project'],
        deltas_annotation_key=config['deltas_annotation_key'],
    )

    loop = asyncio.get_event_loop()

    main_task = asyncio.ensure_future(daemon(config))

    _log = _logger.new(loop=loop, main_task=main_task)

    def handle_signal(name, timeout=10):
        _log.info('Received signal', signal_name=name)

        if main_task.cancelled():
            _log.info('main task already cancelled, forcing a quit')
            return

        _log.info(
            'Cancelling main task',
            task_cancel=main_task.cancel()
        )

    for sig_name in ['SIGINT', 'SIGTERM']:
        loop.add_signal_handler(
            getattr(signal, sig_name),
            functools.partial(handle_signal, sig_name))

    loop.add_signal_handler(signal.SIGUSR1, print_tasks)

    try:
        loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        _log.exception('main task cancelled')
    except Exception as exc:
        _log.exception('Unhandled exception in main task')
        raise
    finally:
        loop.run_until_complete(shutdown(loop=loop))


def debug_excepthook(exc_type, exc, exc_tb):
    import pdb
    loop = asyncio.get_event_loop()
    loop.stop()
    pdb.post_mortem(exc_tb)
    sys.__excepthook__(exc_type, exc, exc_tb)


_shutdown = False


async def shutdown(*, loop=None):
    _logger = structlog.get_logger()
    global _shutdown
    if _shutdown:
        _logger.warning('Already shutting down')
        return

    _shutdown = True

    _logger.debug(
        'shutting down',
    )

    print_tasks()

    _logger.info('Shutdown complete')


def print_tasks():
    tasks = list(asyncio.Task.all_tasks())
    structlog.get_logger().debug('print tasks', tasks=tasks)


if __name__ == '__main__':
    sys.exit(main() or 0)
