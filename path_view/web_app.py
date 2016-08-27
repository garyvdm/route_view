import logging
import hashlib
from functools import partial

import pkg_resources
import uuid
import os
import asyncio

import sockjs
from aiohttp import web
from slugify import slugify

from path_view.core import Path


def make_aio_app(loop, settings, streetview_session):
    app = web.Application(loop=loop)
    app['path_view.settings'] = settings
    app['path_view.streetview_session'] = streetview_session
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
    manager = SessionManagerWithRequest('path_sock', app, path_sock, app.loop)
    sockjs.add_endpoint(app, prefix='/path_sock/{path_id}/', handler=path_sock, manager=manager, name='path_sock')
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
    path = Path(id=path_id, dir_path=path_dir_path,
                streetview_session=app['path_view.streetview_session'], api_key=app['path_view.settings']['api_key'])
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


async def path_sock(msg, session):
    path_sessions = session.request.app['path_view.paths_sessions'][session.path_id]
    path = session.request.app['path_view.paths'][session.path_id]
    if msg.tp == sockjs.MSG_OPEN:
        path_sessions.append(session)
    elif msg.tp == sockjs.MSG_CLOSED:
        path_sessions.remove(session)
    # elif msg.tp == sockjs.MSG_MESSAGE:
    #     session.manager.broadcast(msg.data)


def set_process_task(app, path_id, process_task):
    process_task = asyncio.ensure_future(process_task)
    paths_process_tasks = app['path_view.paths_process_tasks']
    assert path_id not in paths_process_tasks
    paths_process_tasks[path_id] = process_task
    process_task.add_done_callback(partial(process_task_done_callback, path_id))


def process_task_done_callback(path_id, fut):
    try:
        fut.result()
        logging.info("Done processing.")
    except Exception:
        logging.exception("Error processing: ")
