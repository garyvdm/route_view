import logging
import hashlib
import pkg_resources
import uuid
import os
import asyncio

import sockjs
from aiohttp import web
from slugify import slugify


def make_aio_app(loop, settings):
    app = web.Application(loop=loop)
    app['path_view.settings'] = settings
    app['path_view.static_etags'] = {}
    app['path_view.path_tasks'] = {}

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
    path_id = str(uuid.uuid4())
    app = request.app
    app['path_view.path_tasks'][path_id] = asyncio.ensure_future(process_upload(app, path_id, upload_file))
    return web.HTTPFound('/view/{}/'.format(path_id))


async def process_upload(app, path_id, upload_file):
    path_path = os.path.join(app['path_view.settings']['paths_path'], path_id)
    os.mkdir(path_path)

    with open(os.path.join(path_path, 'upload.gpx'), 'wb') as f:
        f.write(upload_file)


# Hack to make session include first request.
class SessionManagerWithRequest(sockjs.SessionManager):
    def get(self, id, create=False, request=None, default=sockjs.session._marker):
        session = super().get(id, create=create, request=request, default=default)
        if not hasattr(session, 'request'):
            session.request = request
        return session


async def path_sock(msg, session):
    if msg.tp == sockjs.MSG_OPEN:
        print(session.request.match_info)
    #     session.manager.broadcast("Someone joined.")
    # elif msg.tp == sockjs.MSG_MESSAGE:
    #     session.manager.broadcast(msg.data)
    # elif msg.tp == sockjs.MSG_CLOSED:
    #     session.manager.broadcast("Someone left.")
