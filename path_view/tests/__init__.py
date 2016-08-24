import unittest

import path_view


def suite():
    tests = unittest.defaultTestLoader.discover(path_view.__path__[0])
    return unittest.TestSuite(tests)
