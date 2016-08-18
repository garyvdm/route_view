import logging
import hashlib
import pkg_resources

from aiohttp import web
from slugify import slugify


def make_aio_app(loop, settings):
    app = web.Application(loop=loop)
    app['cfs.settings'] = settings

    if settings['debugtoolbar']:
        try:
            import aiohttp_debugtoolbar
        except ImportError:
            logging.error('aiohttp_debugtoolbar is enabled, but not installed.')
        else:
            aiohttp_debugtoolbar.setup(app, **settings.get('debugtoolbar_settings', {}))

    app['path_view.static_etags'] = {}
    add_static_resource(
        app, 'static/upload_form.html', 'GET', '/',
        content_type='text/html', charset='utf8',  )

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

    path = path.format(etag[:6])
    app.router.add_route(method, path, static_resource_handler, name=slugify(resource_name))
    return static_resource_handler
