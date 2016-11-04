import unittest
import textwrap
import tempfile

from route_view.core import IndexedPoint, Route
from route_view.tests import unittest_run_loop


class TestGpx(unittest.TestCase):

    @unittest_run_loop
    async def test_load_route_from_gpx(self):

        gpx = textwrap.dedent("""
            <?xml version="1.0"?>
            <gpx version="1.1" xmlns="http://www.topografix.com/GPX/1/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd">
            <trk>
              <name>Test GPX route</name>
              <trkseg>
                <trkpt lat="-26.09321" lon="27.9813"></trkpt>
                <trkpt lat="-26.0933" lon="27.98154"></trkpt>
                <trkpt lat="-26.09341" lon="27.98186"></trkpt>
              </trkseg>
            </trk>
            </gpx>
        """).lstrip('\n').encode()

        with tempfile.TemporaryDirectory() as tempdir:
            route = Route(id=self.id(), name='foobar', dir_route=tempdir, change_callback=lambda foo: None)
            await route.load_route_from_gpx(gpx)
            expected_points = [
                IndexedPoint(lat=-26.09321, lng=27.98130, index=0, distance=0),
                IndexedPoint(lat=-26.09330, lng=27.98154, index=1, distance=25.99741939049353),
                IndexedPoint(lat=-26.09341, lng=27.98186, index=2, distance=60.25098280725716),
            ]
            self.assertEqual(route.route_points, expected_points)
            self.assertEqual(route.name, 'Test GPX route')
