import base64
import contextlib
import hashlib
import io
import json
import logging
import os
from collections import defaultdict
from functools import partial

import attr
import lmdb
import pkg_resources
from aiohttp import web, WSMsgType
from htmlwrite import Tag, Writer
from markupsafe import Markup
from slugify import slugify

import route_view.auth
from route_view.async_exit_stack import AsyncExitStack
from route_view.core import Point, Route
from route_view.util import mk_id


async def make_aio_app(settings):
    app = web.Application()
    app['route_view.settings'] = settings
    app['route_view.static_etags'] = {}
    app['route_view.routes'] = {}
    app['route_view.routes_sessions'] = defaultdict(list)

    add_static = partial(add_static_resource, app)
    add_static('static/view.js', '/static/view.js', content_type='application/javascript', charset='utf8',)
    add_static('static/media-playback-start-symbolic.png', '/static/play.png', content_type='image/png')
    add_static('static/media-playback-pause-symbolic.png', '/static/pause.png', content_type='image/png')

    route_view_static = add_static(
        'static/view.html', None, content_type='text/html', charset='utf8',
        body_processor=lambda app, body: body.decode('utf8').format(api_key=settings['api_key']).encode('utf8'))

    app.router.add_route('GET', '/', home)
    app.router.add_route('POST', '/upload', upload_route)
    app.router.add_route('GET', '/route_sock/{route_id}/', handler=route_ws, name='route_ws')
    app.router.add_route('GET', '/view/{route_id}/', handler=partial(route_view_handler, route_view_static), name='route_view')
    app.router.add_route('GET', '/img/{pano_id_and_heading}', handler=img_handler, name='img')

    route_view.auth.config_aio_app(app, settings)

    app['stack'] = app_stack = AsyncExitStack()
    await app_stack.__aenter__()
    app.on_shutdown.append(shutdown)

    with contextlib.suppress(FileExistsError):
        os.mkdir(settings['data_path'])
    with contextlib.suppress(FileExistsError):
        os.mkdir(os.path.join(settings['data_path'], 'routes'))

    lmdb_env = await app_stack.enter_context(lmdb.open(settings['lmdb_path'], max_dbs=10, map_size=settings['lmdb_map_size']))
    app['route_view.google_api'] = await app_stack.enter_context(route_view.core.GoogleApi(settings['api_key'], lmdb_env))

    return app


async def home(request):
    writer = Writer(io.StringIO())
    w = writer.w
    c = writer.c
    w(Markup('<!DOCTYPE html>'))
    with c(Tag('html')):
        with c(Tag('head')):
            w(Tag('title', c='Route View'))
        with c(Tag('body')):
            await route_view.auth.render_login(request, writer)
            w(Tag('br'))
            with c(Tag('form', action="/upload", method="post", accept_charset="utf-8", enctype="multipart/form-data")):
                w(Tag('label', for_="gpx", c='GPX 1.1 File:'))
                w(Tag('input', id="gpx", name="gpx", type="file", value=""))
                w(Tag('input', type="submit", value="submit"))

            user = await route_view.auth.get_user_or_login(request)
            if user.routes:
                w(Tag('h5', c='Routes'))
                for route_id in user.routes:
                    route = await load_route(request.app, route_id)
                    with c(Tag('li')):
                        w(Tag('a', href='/view/{}/'.format(route_id), c=route.name))

    return web.Response(text=writer.out_file.getvalue(), content_type='text/html')


async def shutdown(app):
    for route in app['route_view.routes'].values():
        if route.process_task:
            route.process_task.cancel()
    for route in app['route_view.routes'].values():
        if route.process_task:
            try:
                await route.process_task
            except Exception:
                pass
    await app['stack'].__aexit__(None, None, None)


def add_static_resource(app, resource_name, route, *args, **kwargs):
    body = pkg_resources.resource_string('route_view', resource_name)
    body_processor = kwargs.pop('body_processor', None)
    if body_processor:
        body = body_processor(app, body)
    kwargs['body'] = body
    headers = kwargs.setdefault('headers', {})
    etag = base64.urlsafe_b64encode(hashlib.sha1(body).digest()).decode('ascii')
    headers['ETag'] = etag
    app['route_view.static_etags'][resource_name] = etag

    async def static_resource_handler(request):
        if request.headers.get('If-None-Match', '') == etag:
            return web.Response(status=304)
        else:
            # TODO check etag query string
            return web.Response(*args, **kwargs)

    # route = route.format(etag[:6])
    if route:
        app.router.add_route('GET', route, static_resource_handler, name=slugify(resource_name))
    return static_resource_handler


async def upload_route(request):
    app = request.app
    data = await request.post()
    upload_file = data['gpx'].file.read()
    name = data['gpx'].filename
    user = await route_view.auth.get_user_or_login(request)

    route_id = mk_id()
    route_dir_route = os.path.join(app['route_view.settings']['data_path'], 'routes', route_id)
    os.mkdir(route_dir_route)
    route = Route(
        id=route_id, name=name, dir_route=route_dir_route,
        change_callback=partial(change_callback, request.app['route_view.routes_sessions'][route_id]),
        google_api=app['route_view.google_api'], owner=user.id)
    app['route_view.routes'][route_id] = route
    await route.load_route_from_upload(upload_file)
    await route.save_metadata()
    await route.start_processing()
    user = await route_view.auth.get_user_or_login(request)
    user.routes.append(route_id)
    await user.save()
    return web.HTTPFound('/view/{}/'.format(route_id))


async def load_route(app, route_id):
    route = app['route_view.routes'].get(route_id)
    if route is None:
        route_dir_route = os.path.join(app['route_view.settings']['data_path'], 'routes', route_id)
        try:
            route = await (Route.load(route_id, route_dir_route, partial(change_callback, app['route_view.routes_sessions'][route_id])))
        except FileNotFoundError as e:
            raise KeyError() from e

        route.google_api = app['route_view.google_api']
        app['route_view.routes'][route_id] = route
    return route


async def request_has_access_to_route(request, route):
    if not route.private:
        return True
    user = await route_view.auth.get_user_or_login(request)
    if user.admin:
        return True
    return route.id in user.routes


async def route_view_handler(route_view_static, request):
    route_id = request.match_info['route_id']
    try:
        route = await load_route(request.app, route_id)
    except KeyError:
        writer = Writer(io.StringIO())
        w = writer.w
        c = writer.c
        w(Markup('<!DOCTYPE html>'))
        with c(Tag('html')):
            with c(Tag('head')):
                w(Tag('title', c='Route not found'))
            with c(Tag('body')):
                w(Tag('h1', c='This route does not exist.'))
                await route_view.auth.render_login(request, writer)
        return web.Response(status=404, text=writer.out_file.getvalue(), content_type='text/html')

    if not await request_has_access_to_route(request, route):
        writer = Writer(io.StringIO())
        w = writer.w
        c = writer.c
        w(Markup('<!DOCTYPE html>'))
        with c(Tag('html')):
            with c(Tag('head')):
                w(Tag('title', c='Route Permission Denied'))
            with c(Tag('body')):
                w(Tag('h1', c='You do not have permission to view this route.'))
                await route_view.auth.render_login(request, writer)
        return web.Response(status=403, text=writer.out_file.getvalue(), content_type='text/html')
    else:
        return await route_view_static(request)


async def route_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    route_id = request.match_info['route_id']
    route = await load_route(request.app, route_id)

    if not await request_has_access_to_route(request, route):
        await ws.send_str(json.dumps({'error': 'no permission'}))
        await ws.close()
        return ws

    await route.ensure_data_loaded()

    route_sessions = request.app['route_view.routes_sessions'][route_id]
    route_sessions.append(ws)

    # Send initial data.
    await ws.send_str(json.dumps({'api_key': request.app['route_view.google_api'].api_key}))
    for msg in route.get_existing_changes():
        await ws.send_str(json.dumps(msg, default=json_encode))

    try:
        async for msg in ws:
            if msg.type == WSMsgType.text:
                data = json.loads(msg.data)
                # logging.debug(data)
                if data == 'cancel':
                    await route.cancel_processing()
                if data == 'resume':
                    await route.resume_processing()
                if isinstance(data, dict) and 'add_pano_chain_item' in data:
                    await route.add_pano_chain_item(*data['add_pano_chain_item'])
            if msg.type == WSMsgType.close:
                await ws.close()
            if msg.type == WSMsgType.error:
                raise ws.exception()
    finally:
        route_sessions.remove(ws)
    return ws


async def change_callback(route_sessions, change):
    msg = json.dumps(change, default=json_encode)
    # logging.debug(str(change)[:120])
    for session in route_sessions:
        try:
            await session.send_str(msg)
        except Exception:
            logging.exception('Error sending to client: ')


point_json_attrs = {'lat', 'lng'}


def json_encode(obj):
    if isinstance(obj, Point):
        return attr.asdict(obj, filter=lambda a, v: a.name in point_json_attrs)


async def img_handler(request):
    pano_id, _, heading = request.match_info['pano_id_and_heading'].rpartition('~')
    heading = float(heading)
    img = await request.app['route_view.google_api'].get_pano_img(pano_id, heading)
    etag = base64.urlsafe_b64encode(hashlib.sha1(img).digest()).decode('ascii')

    if request.headers.get('If-None-Match') == etag:
        # since these images can be cached indefinitely, return not modified
        return web.Response(status=304)
    else:

        return web.Response(body=img, headers=(
            ('Cache-Control', 'public, max-age=31536000'),
            ('Content-Type', 'image/jpeg'),
            ('ETag', etag),
        ))
