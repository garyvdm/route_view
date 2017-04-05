import argparse
import copy
import logging.config
import asyncio
import socket
import contextlib
import os
import signal
import sys

import uvloop
import yaml
import lmdb

import route_view.web_app
import route_view.core

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
            route_view:
                 level: INFO
                 qualname: route_view

            aiohttp:
                 level: INFO
                 qualname: aiohttp

            asyncio:
                 level: INFO
                 qualname: asyncio

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

        with contextlib.suppress(FileExistsError):
            os.mkdir(settings['data_path'])
        with contextlib.suppress(FileExistsError):
            os.mkdir(os.path.join(settings['data_path'], 'routes'))

        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, signame), loop.stop)

        try:
            with contextlib.ExitStack() as stack:
                lmdb_env = stack.enter_context(lmdb.open(settings['lmdb_path'], max_dbs=10, map_size=settings['lmdb_map_size']))
                google_api = stack.enter_context(route_view.core.GoogleApi(settings['api_key'], lmdb_env, asyncio.get_event_loop()))
                stack.enter_context(web_serve_cm(loop, settings, google_api))
                try:
                    loop.run_forever()
                except KeyboardInterrupt:
                    pass
        finally:
            loop.close()
    except Exception:
        logging.exception('Unhandled exception:')
        sys.exit(3)


@contextlib.contextmanager
def web_serve_cm(loop, settings, google_api):
    app = route_view.web_app.make_aio_app(loop, settings, google_api)

    handler = app.make_handler(debug=settings.get('aioserver_debug', False))

    if settings['server_type'] == 'inet':
        srv = loop.run_until_complete(loop.create_server(handler, settings['inet_host'], settings['inet_port']))
    elif settings['server_type'] == 'unix':
        unix_path = settings['unix_path']
        if os.path.exists(unix_path):
            try:
                os.unlink(unix_path)
            except OSError:
                logging.exception("Could not unlink socket '{}'".format(unix_path))
        srv = loop.run_until_complete(loop.create_unix_server(handler, unix_path))
        os.chmod(unix_path, 660)

    for sock in srv.sockets:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            print('Serving on http://{}:{}'.format(*sock.getsockname()))
            app.setdefault('host_urls', []).append('http://{}:{}'.format(*sock.getsockname()))
        else:
            print('Serving on {!r}'.format(sock))

    try:
        yield
    finally:
        loop.run_until_complete(route_view.web_app.app_cancel_processing(app))
        loop.run_until_complete(handler.finish_connections(1.0))
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        loop.run_until_complete(app.finish())
