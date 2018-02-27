import contextlib
import datetime
import os

import attr
import cachetools
import yaml
from aioauth_client import (
    ClientRegistry,
    OAuth1Client,
    OAuth2Client,
)
from aiohttp import web
from htmlwrite import Tag

from route_view.util import (
    mk_id,
    runs_in_executor,
)


class StravaClient(OAuth2Client):

    """Support Strava.

    * Dashboard: https://www.strava.com/settings/api
    * Docs: https://strava.github.io/api/v3/oauth/
    """

    access_token_url = 'https://www.strava.com/oauth/token'
    authorize_url = 'https://www.strava.com/oauth/authorize'
    user_info_url = 'https://www.strava.com/api/v3/athlete'
    name = 'strava'

    @staticmethod
    def user_parse(data):
        """Parse information from provider."""
        yield 'id', data.get('sub') or data.get('id')
        yield 'username', data.get('username')
        yield 'first_name', data.get('firstname')
        yield 'last_name', data.get('lastname')
        yield 'locale', data.get('language')
        yield 'link', data.get('url')
        yield 'picture', data.get('profile')
        yield 'email', data.get('email')


@attr.s
class StorageType(object):
    app = attr.ib()
    path = attr.ib()
    id = attr.ib()

    no_save_attrs = {'app', 'path'}

    @classmethod
    async def load(cls, app, id):
        cache = app['route_view.{}_cache'.format(cls.type_name)]
        try:
            return cache[id]
        except KeyError:
            item = await cls._load(app, id)
            cache[id] = item
            return item

    @classmethod
    @runs_in_executor
    def _load(cls, app, id):
        path = os.path.join(app['route_view.{}_route'.format(cls.type_name)], id)
        try:
            with open(path, 'r') as f:
                data = yaml.load(f)
            return cls(app, path, **data)
        except FileNotFoundError:
            login = cls(app, path, id)
            return login

    @runs_in_executor
    def save(self):
        data = attr.asdict(self, filter=lambda a, v: a.name not in self.no_save_attrs)
        with open(self.path, 'w') as f:
            yaml.dump(data, f)


@attr.s
class Login(StorageType):
    type_name = 'logins'
    user_id = attr.ib(default=None)
    routes = attr.ib(default=attr.Factory(list))
    creation_timestamp = attr.ib(default=attr.Factory(datetime.datetime.now))
    access_timestamp = attr.ib(default=attr.Factory(lambda: datetime.datetime.fromtimestamp(0)))
    access_timestamp_change_delta = datetime.timedelta(hours=1)
    admin = False

    async def access(self):
        now = datetime.datetime.now()
        if now - self.access_timestamp - self.access_timestamp_change_delta:
            self.access_timestamp = now
            await self.save()

    @runs_in_executor
    def save(self):
        if not self.routes and not self.user_id:
            with contextlib.suppress(FileNotFoundError):
                os.remove(self.path)
        else:
            super().save.__wrapped__(self)


@attr.s
class OAuthID(StorageType):
    type_name = 'oauthids'
    user_id = attr.ib(default=None)


@attr.s
class User(StorageType):
    type_name = 'users'
    primary_oauthid = attr.ib(default=None)
    oauth_details = attr.ib(default=attr.Factory(dict))
    tokens = attr.ib(default=attr.Factory(dict))
    routes = attr.ib(default=attr.Factory(list))
    admin = attr.ib(default=False)


async def login_middleware_factory(app, handler):
    async def login_middleware(request):
        login_id = request.cookies.get('login')
        login_needs_to_be_set = login_id is None
        if login_needs_to_be_set:
            login_id = mk_id()

        login = await Login.load(app, login_id)
        await login.access()
        request['route_view.login'] = login
        request['route_view.login_needs_to_be_set'] = login_needs_to_be_set
        return await handler(request)
    return login_middleware


async def login_on_prepare(request, response):
    if request['route_view.login_needs_to_be_set']:
        response.set_cookie('login', request['route_view.login'].id)


def config_aio_app(app, settings):
    data_path = settings['data_path']
    storage_types = (Login, User, OAuthID)
    app['route_view.oauth_providers'] = settings['oauth_providers']
    app['route_view.oauth_providers_by_name'] = {provider['name']: provider for provider in settings['oauth_providers']}

    for cls in storage_types:
        route = os.path.join(data_path, cls.type_name)
        with contextlib.suppress(FileExistsError):
            os.mkdir(route)
        app['route_view.{}_route'.format(cls.type_name)] = route
        app['route_view.{}_cache'.format(cls.type_name)] = cachetools.LRUCache(128)

    app.middlewares.append(login_middleware_factory)
    app.on_response_prepare.append(login_on_prepare)
    # clients = settings['oauth_clients']
    app.router.add_route('GET', '/oauth/{provider}', oauth)
    app.router.add_route('GET', '/logout', logout)


async def render_login(request, writer):
    c = writer.c
    w = writer.w
    login = request['route_view.login']
    with c(Tag('div', class_='login')):
        if login.user_id is None:
            w('You are not logged in. It is recommended you login so that you can easily find your routes in the future. Click one of the links to login: ')
            for provider in request.app['route_view.oauth_providers']:
                w(Tag('a', href='/oauth/{}'.format(provider['name']), c=provider['display_name']))
            w(Tag('br'))
            w('Tip: if you login with Strava, you will be able to import routes and activities from Strava.')
        else:
            user = await User.load(request.app, login.user_id)
            oauthids = ', '.join(user.oauth_details)
            user_details = user.oauth_details[user.primary_oauthid]
            w('You are logged in as {0[first_name]} {0[last_name]} {1}.'.format(user_details, oauthids))
            w(Tag('a', href='/logout', c='Logout'))


async def get_user_or_login(request):
    login = request['route_view.login']
    if login.user_id:
        return await User.load(request.app, login.user_id)
    return login


async def oauth(request):
    providers_by_name = request.app['route_view.oauth_providers_by_name']
    provider = request.match_info.get('provider')
    if provider not in providers_by_name:
        raise web.HTTPNotFound(reason='Unknown provider')

    # Create OAuth1/2 client
    Client = ClientRegistry.clients[provider]
    params = providers_by_name[provider]['init']
    client = Client(**params)
    client.params['oauth_callback' if issubclass(Client, OAuth1Client) else 'redirect_uri'] = \
        'http://%s%s' % (request.host, request.path)

    # Check if is not redirect from provider
    if client.shared_key not in request.query:

        # For oauth1 we need more work
        if isinstance(client, OAuth1Client):
            token, secret, _ = await client.get_request_token()

            # Dirty save a token_secret
            # Dont do it in production
            request.app.secret = secret
            request.app.token = token

        # Redirect client to provider
        return web.HTTPFound(client.get_authorize_url())

    # For oauth1 we need more work
    if isinstance(client, OAuth1Client):
        client.oauth_token_secret = request.app.secret
        client.oauth_token = request.app.token

    token = (await client.get_access_token(request.query))[0]
    user_provider_details, user_data = await client.user_info()
    oauthid_id = '{}:{}'.format(client.name, user_provider_details.username or user_provider_details.id)
    oauthid = await OAuthID.load(request.app, oauthid_id)
    # TODO merging of users / logins
    if oauthid.user_id is None:
        oauthid.user_id = mk_id()
        await oauthid.save()
    login = request['route_view.login']
    login.user_id = oauthid.user_id
    user = await User.load(request.app, oauthid.user_id)
    if user.primary_oauthid is None:
        user.primary_oauthid = oauthid.id
    user.oauth_details[oauthid.id] = dict(client.user_parse(user_data))
    user.tokens[oauthid.id] = token
    await user.save()

    return web.HTTPFound('/')


async def logout(request):
    login = request['route_view.login']
    login.user_id = None
    await login.save()
    return web.HTTPFound('/')
