import argparse
import copy
import logging.config
import asyncio
import socket
import contextlib
import os

import uvloop
import yaml

import path_view.web_app
import path_view.core

defaults_yaml = """
    server_type: inet
    inet_host: ''
    inet_port: 6841
    debugtoolbar: False
    aioserver_debug: False
    data_path: data
    api_cache_db: data/api_cache


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
            path_view:
                 level: INFO
                 qualname: path_view

            aiohttp:
                 level: INFO
                 qualname: aiohttp

            asyncio:
                 level: INFO
                 qualname: asyncio

"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('settings_file', action='store', nargs='?', default='/etc/path_view.yaml',
                        help='File to load settings from.')
    parser.add_argument('--inet', action='store',
                        help='Host address and port to listen on. (format: host:port)')
    parser.add_argument('--unix', action='store',
                        help='Path of unix socket to listen on. ')
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
        os.mkdir(os.path.join(settings['data_path'], 'paths'))

    with contextlib.ExitStack() as stack:
        google_api = stack.enter_context(path_view.core.GoogleApi(settings['api_key'], settings['api_cache_db'], asyncio.get_event_loop()))

        stack.enter_context(web_serve_cm(loop, settings, google_api))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

    loop.close()


@contextlib.contextmanager
def web_serve_cm(loop, settings, google_api):
    app = path_view.web_app.make_aio_app(loop, settings, google_api)

    handler = app.make_handler(debug=settings.get('aioserver_debug', False))

    if settings['server_type'] == 'inet':
        srv = loop.run_until_complete(loop.create_server(handler, settings['inet_host'], settings['inet_port']))
    elif settings['server_type'] == 'unix':
        srv = loop.run_until_complete(loop.create_unix_server(handler, settings['unix_path']))

    for sock in srv.sockets:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            print('Serving on http://{}:{}'.format(*sock.getsockname()))
            app.setdefault('host_urls', []).append('http://{}:{}'.format(*sock.getsockname()))
        else:
            print('Serving on {!r}'.format(sock))

    try:
        yield
    finally:
        loop.run_until_complete(path_view.web_app.app_cancel_processing(app))
        loop.run_until_complete(handler.finish_connections(1.0))
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        loop.run_until_complete(app.finish())
