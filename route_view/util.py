import asyncio
import base64
import functools
import uuid


def runs_in_executor(fn):

    @functools.wraps(fn)
    async def runs_in_executor_inner(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))

    return runs_in_executor_inner


def mk_id():
    return id_encode(uuid.uuid4().bytes).decode('ascii')


def id_encode(id):
    return base64.urlsafe_b64encode(id)[:22]


def id_decode(id):
    if isinstance(id, str):
        id = id.encode('ascii')
    return base64.urlsafe_b64decode(id + b'==')
