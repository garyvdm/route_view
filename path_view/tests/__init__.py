import unittest
import functools
import asyncio
import logging

import path_view

logging.basicConfig(level=logging.DEBUG)


def suite():
    tests = unittest.defaultTestLoader.discover(path_view.__path__[0])
    return unittest.TestSuite(tests)


def unittest_run_loop(func):
    """a decorator that should be used with asynchronous methods of an
    AioHTTPTestCase. Handles executing an asynchronous function, using
    the self.loop of the AioHTTPTestCase.
    """

    @functools.wraps(func)
    def new_func(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(func(self))

    return new_func
