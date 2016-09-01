import unittest
import textwrap

from path_view.core import gpx_get_points, IndexedPoint


class TestGpx(unittest.TestCase):
    def test_get_points(self):
        gpx = textwrap.dedent("""
            <?xml version="1.0"?>
            <gpx version="1.1" xmlns="http://www.topografix.com/GPX/1/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd">
            <trk>
              <trkseg>
                <trkpt lat="-26.09321" lon="27.9813"></trkpt>
                <trkpt lat="-26.0933" lon="27.98154"></trkpt>
                <trkpt lat="-26.09341" lon="27.98186"></trkpt>
              </trkseg>
            </trk>
            </gpx>
        """).lstrip('\n')
        points = gpx_get_points(gpx)
        expected_points = [
            IndexedPoint(lat=-26.09321, lng=27.98130, index=0, distance=0),
            IndexedPoint(lat=-26.09330, lng=27.98154, index=1, distance=25.99741939049353),
            IndexedPoint(lat=-26.09341, lng=27.98186, index=2, distance=60.25098280725716),
        ]
        self.assertEqual(points, expected_points)
