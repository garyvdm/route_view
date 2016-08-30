import logging
import hashlib
import json
import pkg_resources
import uuid
import os
from collections import defaultdict
from functools import partial

import attr
from aiohttp import web, MsgType
from slugify import slugify

from path_view.core import Path, Point


def make_aio_app(loop, settings, google_api):
    app = web.Application(loop=loop)
    app['path_view.settings'] = settings
    app['path_view.google_api'] = google_api
    app['path_view.static_etags'] = {}
    app['path_view.paths'] = {}
    app['path_view.paths_sessions'] = defaultdict(list)

    if settings['debugtoolbar']:
        try:
            import aiohttp_debugtoolbar
        except ImportError:
            logging.error('aiohttp_debugtoolbar is enabled, but not installed.')
        else:
            aiohttp_debugtoolbar.setup(app, **settings.get('debugtoolbar_settings', {}))

    add_static_resource(
        app, 'static/upload_form.html', 'GET', '/',
        content_type='text/html', charset='utf8',)
    add_static_resource(
        app, 'static/view.html', 'GET', '/view/{path_id}/',
        content_type='text/html', charset='utf8',)
    add_static_resource(
        app, 'static/view.js', 'GET', '/static/view.js',
        content_type='application/javascript', charset='utf8',)

    app.router.add_route('POST', '/upload', upload_path)
    app.router.add_route('*', '/path_sock/{path_id}/', handler=path_ws, name='path_ws')
    return app


async def app_cancel_processing(app):
    for path in app['path_view.paths'].values():
        if path.process_task:
            path.process_task.cancel()
    for path in app['path_view.paths'].values():
        if path.process_task:
            try:
                await path.process_task
            except Exception:
                pass


def add_static_resource(app, resource_name, method, path, *args, **kwargs):
    body = pkg_resources.resource_string('path_view', resource_name)
    body_processor = kwargs.pop('body_processor', None)
    if body_processor:
        body = body_processor(app, body)
    kwargs['body'] = body
    headers = kwargs.setdefault('headers', {})
    etag = hashlib.sha1(body).hexdigest()
    headers['ETag'] = etag
    app['path_view.static_etags'][resource_name] = etag

    def static_resource_handler(request):
        if request.headers.get('If-None-Match', '') == etag:
            return web.Response(status=304)
        else:
            # TODO check etag query string
            return web.Response(*args, **kwargs)

    # path = path.format(etag[:6])
    app.router.add_route(method, path, static_resource_handler, name=slugify(resource_name))
    return static_resource_handler

async def upload_path(request):
    data = await request.post()
    upload_file = data['gpx'].file.read()
    name = data['gpx'].filename
    app = request.app
    path_id = str(uuid.uuid4())
    path_dir_path = os.path.join(app['path_view.settings']['paths_path'], path_id)
    path = Path(id=path_id, name=name, dir_path=path_dir_path,
                change_callback=partial(change_callback, request.app['path_view.paths_sessions'][path_id]))
    app['path_view.paths'][path_id] = path
    await path.load_route_from_gpx(upload_file)
    await path.start_processing(app['path_view.google_api'])
    return web.HTTPFound('/view/{}/'.format(path_id))


async def path_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    path_id = request.match_info['path_id']
    path = request.app['path_view.paths'].get(path_id)
    if path is None:
        path_dir_path = os.path.join(request.app['path_view.settings']['paths_path'], path_id)
        path = await (Path.load(path_id, path_dir_path, partial(change_callback, request.app['path_view.paths_sessions'][path_id])))
        request.app['path_view.paths'][path_id] = path
        await path.ensure_data_loaded()

    path_sessions = request.app['path_view.paths_sessions'][path_id]
    path_sessions.append(ws)

    # Send initial data.
    for msg in path.get_existing_changes():
        ws.send_str(json.dumps(msg, default=json_encode))

    try:
        async for msg in ws:
            if msg.tp == MsgType.text:
                if msg.data == 'close':
                    await ws.close()
                else:
                    pass
            elif msg.tp == MsgType.error:
                raise ws.exception()
    finally:
        path_sessions.remove(ws)
    return ws


def change_callback(path_sessions, change):
    msg = json.dumps(change, default=json_encode)
    for session in path_sessions:
        try:
            session.send_str(msg)
        except:
            logging.exception('Error sending to client: ')

point_json_attrs = {'lat', 'lng'}


def json_encode(obj):
    if isinstance(obj, Point):
        return attr.asdict(obj, filter=lambda a, v: a.name in point_json_attrs)
