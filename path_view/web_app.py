import logging
import hashlib
import json
from functools import partial

import pkg_resources
import uuid
import os
import asyncio

from aiohttp import web, MsgType
from slugify import slugify

from path_view.core import Path, Point


def make_aio_app(loop, settings, google_api):
    app = web.Application(loop=loop)
    app['path_view.settings'] = settings
    app['path_view.google_api'] = google_api
    app['path_view.static_etags'] = {}
    app['path_view.paths'] = {}
    app['path_view.paths_process_tasks'] = {}
    app['path_view.paths_sessions'] = {}

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
    app.router.add_route('POST', '/upload', upload_path)
    add_static_resource(
        app, 'static/view.html', 'GET', '/view/{path_id}/',
        content_type='text/html', charset='utf8',)
    add_static_resource(
        app, 'static/view.js', 'GET', '/static/view.js',
        content_type='application/javascript', charset='utf8',)
    app.router.add_route('*', '/path_sock/{path_id}/', handler=path_ws, name='path_ws')
    return app


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
    app = request.app
    path_id = str(uuid.uuid4())
    path_dir_path = os.path.join(app['path_view.settings']['paths_path'], path_id)
    sessions = []
    request.app['path_view.paths_sessions'][path_id] = sessions
    path = Path(id=path_id, dir_path=path_dir_path,
                google_api=app['path_view.google_api'],
                new_pano_callback=partial(new_pano_callback, sessions))
    app['path_view.paths'][path_id] = path
    set_process_task(request.app, path_id, path.process_upload(upload_file))
    return web.HTTPFound('/view/{}/'.format(path_id))




async def path_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    path_id = request.match_info['path_id']
    path = request.app['path_view.paths'][path_id]
    path_sessions = request.app['path_view.paths_sessions'][path_id]
    path_sessions.append(ws)

    # Send initial data.
    ws.send_str(json.dumps({'panos': path.panos, }, default=json_encode))


    async for msg in ws:
        if msg.tp == MsgType.text:
            if msg.data == 'close':
                await ws.close()
                path_sessions.remove(ws)
            else:
                pass
        elif msg.tp == MsgType.error:
            raise ws.exception()
    return ws



def set_process_task(app, path_id, process_task):
    process_task = asyncio.ensure_future(process_task)
    paths_process_tasks = app['path_view.paths_process_tasks']
    assert path_id not in paths_process_tasks
    paths_process_tasks[path_id] = process_task
    sessions = app['path_view.paths_sessions'][path_id]
    process_task.add_done_callback(partial(process_task_done_callback, sessions))


def process_task_done_callback(path_sessions, fut):
    try:
        fut.result()
        logging.info("Done processing.")
    except Exception as e:
        logging.exception("Error processing: ")


def new_pano_callback(path_sessions, pano):
    msg = json.dumps({'panos': [pano], }, default=json_encode)
    for session in path_sessions:
        session.send_str(msg)

def json_encode(obj):
    if isinstance(obj, Point):
        return (obj.lat, obj.lng)
