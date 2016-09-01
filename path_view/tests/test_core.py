import unittest
import tempfile
import contextlib
import os
import asyncio
import pprint

from path_view.tests import unittest_run_loop
from path_view.core import (
    Point,
    Path,
    path_with_distance_and_index,
    find_closest_point_pair,
    iter_points_with_minimal_spacing,
    geo_from_distance_on_path,
    GoogleApi,
)


class TestHelpers(unittest.TestCase):

    def test_find_closest_point_pair(self):
        points = [Point(0, 0), Point(0, 30), Point(0, 60), Point(0, 90), Point(0, 120)]
        closest_point_pair, cpoint, dist = find_closest_point_pair(points, Point(0.0001, 45))
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

    def test_iter_points_with_minimal_spacing_close_points(self):
        # Points that a closer together then the spacing value should work too.
        points = [Point(0, 0), Point(0, 0.0000001)]
        points_with_minimal_spacing = list(iter_points_with_minimal_spacing(points, spacing=4000))
        self.assertEqual(len(points_with_minimal_spacing), 2)

    def test_point_from_distance_on_path(self):
        path = [Point(0, 0), Point(0, 0.0001), Point(0.0001, 0.0001)]
        print(geo_from_distance_on_path(path, 10))
        geo10 = geo_from_distance_on_path(path, 10)
        geo20 = geo_from_distance_on_path(path, 20)
        self.assertEqual((geo10['lat2'], geo10['lon2']), (0.0, 8.983152841195216e-05))
        self.assertEqual((geo20['lat2'], geo20['lon2']), (8.019994573584536e-05, 0.0001))


class TestPointProcess(unittest.TestCase):

    @contextlib.contextmanager
    def process_stack(self):
        with contextlib.ExitStack() as stack:
            try:
                api_key = os.environ['PATHVIEW_TEST_APIKEY']
            except KeyError:
                raise unittest.SkipTest('PATHVIEW_TEST_APIKEY env key not set.')
            api = stack.enter_context(GoogleApi(api_key, ':mem:', asyncio.get_event_loop()))
            tempdir = stack.enter_context(tempfile.TemporaryDirectory())

            def change_callback(change):
                pprint.pprint(change)

            yield api, tempdir, change_callback

    @unittest_run_loop
    async def test_process1(self):
        with self.process_stack() as (api, tempdir, change_callback):
            path = Path(None, tempdir, change_callback, name='Test Path', )
            await path.save_metadata()
            await path.set_route_points(path_with_distance_and_index([
                (-26.09332, 27.98120),
                (-26.09326, 27.98112),
            ]))
            await path.start_processing(api)
            await path.process_task
            self.assertTrue(path.processing_complete)

            loaded_path = await Path.load(None, tempdir, change_callback)
            await loaded_path.ensure_data_loaded()

            self.maxDiff = None
            self.assertEqual(path.route_points, loaded_path.route_points)
            self.assertEqual(path.panos, loaded_path.panos)

    @unittest_run_loop
    async def test_process2(self):
        # This path would go into an infinate loop at the end. Test to make sure it finishes.

        with self.process_stack() as (api, tempdir, change_callback):
            path = Path(None, tempdir, change_callback, name='Test Path', )
            await path.set_route_points(path_with_distance_and_index([
                (45.03778, 6.92901),
                (45.03790, 6.92922),
                (45.03795, 6.92929),
            ]))
            await path.start_processing(api)
            await path.process_task
            self.assertTrue(path.processing_complete)
