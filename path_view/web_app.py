import logging
import hashlib
import json
from functools import partial

import pkg_resources
import uuid
import os
import asyncio

import sockjs
from aiohttp import web
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
    path_sock_handler = partial(path_sock, app)
    manager = SessionManagerWithRequest('path_sock', app, path_sock_handler, app.loop)
    sockjs.add_endpoint(app, prefix='/path_sock/{path_id}/', handler=path_sock_handler, manager=manager, name='path_sock')
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


# Hack to make session include first request.
class SessionManagerWithRequest(sockjs.SessionManager):
    def get(self, id, create=False, request=None, default=sockjs.session._marker):
        session = super().get(id, create=create, request=request, default=default)
        if not hasattr(session, 'path_id'):
            session.path_id = request.match_info['path_id']
        return session


async def path_sock(app, msg, session):
    path_sessions = app['path_view.paths_sessions'][session.path_id]
    path = app['path_view.paths'][session.path_id]
    if msg.tp == sockjs.MSG_OPEN:
        path_sessions.append(session)
        msg = json.dumps({'panos': path.panos, }, default=json_encode)
        session.send(msg)
    elif msg.tp == sockjs.MSG_CLOSED:
        path_sessions.remove(session)
    # elif msg.tp == sockjs.MSG_MESSAGE:
    #     session.manager.broadcast(msg.data)


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
        session.send(msg)

def json_encode(obj):
    if isinstance(obj, Point):
        return (obj.lat, obj.lng)
