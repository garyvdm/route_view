import argparse
import asyncio
import copy
import logging.config
import os
import shutil
import signal
import sys

import uvloop
import yaml
from aiohttp.web import AppRunner, TCPSite, UnixSite
from yarl import URL

import route_view.core
import route_view.web_app

defaults_yaml = """
    server_type: inet
    inet_host: ''
    inet_port: 6841
    debugtoolbar: False
    aioserver_debug: False
    data_path: data
    lmdb_path: data/lmdb
    lmdb_map_size: 1250000000   # 10 GB

    logging:
        version: 1
        disable_existing_loggers: false
        handlers:
            console:
                formatter: generic
                stream  : ext://sys.stdout
                class : logging.StreamHandler
                level: NOTSET

        formatters:
            generic:
                format: '%(levelname)-5.5s [%(name)s] %(message)s'
        root:
            level: NOTSET
            handlers: [console, ]

        loggers:
            aiohttp.access:
                level: ERROR
                qualname: aiohttp.access

"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('settings_file', action='store', nargs='?', default='/etc/route_view.yaml',
                        help='File to load settings from.')
    parser.add_argument('--inet', action='store',
                        help='Host address and port to listen on. (format: host:port)')
    parser.add_argument('--unix', action='store',
                        help='Route of unix socket to listen on. ')
    parser.add_argument('--dev', action='store_true',
                        help='Enable development tools (e.g. debug toolbar.)')
    parser.add_argument('--api-key', action='store',
                        help='Google api key. ')
    args = parser.parse_args()

    defaults = yaml.load(defaults_yaml)
    settings = copy.deepcopy(defaults)
    try:
        with open(args.settings_file) as f:
            settings_from_file = yaml.load(f)
    except FileNotFoundError:
        settings_from_file = {}
    settings.update(settings_from_file)

    logging.config.dictConfig(settings['logging'])

    try:

        if args.inet:
            host, _, port_str = args.inet.split(':')
            port = int(port_str)
            settings['server_type'] = 'inet'
            settings['inet_host'] = host
            settings['inet_port'] = port
        if args.unix:
            settings['server_type'] = 'unix'
            settings['unix_path'] = args.unix
        if args.dev:
            settings['debugtoolbar'] = True
            settings['aioserver_debug'] = True
        if args.api_key:
            settings['api_key'] = args.api_key

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(serve(loop, settings, route_view.web_app.make_aio_app))
        finally:
            loop.close()
    except Exception:
        logging.exception('Unhandled exception:')
        sys.exit(3)


async def serve(loop, settings, make_app):

    app = await make_app(settings)
    runner = AppRunner(app, debug=settings.get('aioserver_debug', False),
                       access_log_format='%l %u %t "%r" %s %b "%{Referrer}i" "%{User-Agent}i"')
    await runner.setup()

    if settings['server_type'] == 'inet':
        site = TCPSiteSocketName(runner, settings['inet_host'], settings['inet_port'])
    elif settings['server_type'] == 'unix':
        unix_path = settings['unix_path']
        if os.path.exists(unix_path):
            try:
                os.unlink(unix_path)
            except OSError:
                logging.exception("Could not unlink socket '{}'".format(unix_path))
        site = UnixSite(runner, unix_path)
        if 'unix_chmod' in settings:
            os.chmod(unix_path, settings['unix_chmod'])
        if 'unix_chown' in settings:
            shutil.chown(unix_path, **settings['unix_chown'])

    await site.start()

    logging.info(f'Serving on {site.name}')

    try:
        # Run forever (or we get interupt)
        run_fut = asyncio.Future()
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, signame), run_fut.set_result, None)
        try:
            await run_fut
        finally:
            for signame in ('SIGINT', 'SIGTERM'):
                loop.remove_signal_handler(getattr(signal, signame))
    finally:
        await site.stop()
        await runner.cleanup()


class TCPSiteSocketName(TCPSite):

    @property
    def name(self):
        scheme = 'https' if self._ssl_context else 'http'
        socks = [sock.getsockname() for sock in self._server.sockets]
        return [str(URL.build(scheme=scheme, host=sock[0], port=sock[1])) for sock in socks]
