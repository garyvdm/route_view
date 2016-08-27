import unittest
import tempfile
import contextlib
import os

import aiohttp

from path_view.tests import unittest_run_loop
from path_view.core import (
    Point,
    IndexedPoint,
    Path,
    find_closest_point_pair,
    iter_points_with_minimal_spacing,
)


class TestHelpers(unittest.TestCase):

    def test_find_closest_point_pair(self):
        points = [Point(0, 0), Point(0, 30), Point(0, 60), Point(0, 90), Point(0, 120)]
        closest_point_pair, cpoint, distance = find_closest_point_pair(points, Point(0.0001, 45))
        self.assertEqual(closest_point_pair, (Point(0, 30), Point(0, 60)))
        self.assertEqual(cpoint, Point(0, 45))

    def test_iter_points_with_minimal_spacing(self):
        points = [Point(0, 0), Point(0, 0.1), Point(0.1, 0.1)]
        points_with_minimal_spacing = list(iter_points_with_minimal_spacing(points, spacing=4000))
        # bit lazy to do asserts on the acatual values returned here. Just check the the correct number of items
        # are returned
        for point in points_with_minimal_spacing:
            print(point)
        self.assertEqual(len(points_with_minimal_spacing), 5)



class TestPointProcess(unittest.TestCase):

    @unittest_run_loop
    async def test_process(self):

        with contextlib.ExitStack() as stack:
            tempdir = stack.enter_context(tempfile.TemporaryDirectory())
            streetview_session = stack.enter_context(contextlib.closing(aiohttp.ClientSession()))
            try:
                api_key = os.environ['PATHVIEW_TEST_APIKEY']
            except KeyError:
                raise unittest.SkipTest('PATHVIEW_TEST_APIKEY env key not set.')

            def new_pano_callback(pano):
                pass

            path = Path(None, tempdir, streetview_session, api_key, new_pano_callback)
            path.route_points = [
                IndexedPoint(lat=-26.09332, lng=27.9812, index=0),
                IndexedPoint(lat=-26.09326, lng=27.98112, index=1),
                IndexedPoint(lat=-26.09264, lng=27.97940, index=2),
            ]
            await path.process()

