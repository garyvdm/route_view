import functools
import asyncio
import base64
import uuid


def runs_in_executor(fn):

    @functools.wraps(fn)
    async def runs_in_executor_inner(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))

    return runs_in_executor_inner


def mk_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode('ascii')[:22]
