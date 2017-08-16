import asyncio
import sys

from k8s_snapshots.core import daemon


def main():
    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(daemon())
    loop.run_until_complete(main_task)


if __name__ == '__main__':
    sys.exit(main() or 0)
