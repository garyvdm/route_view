import unittest
import textwrap

import path_view.gpx
from path_view.point import Point


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
        points = path_view.gpx.get_points(gpx)
        expected_points = [Point(lat='-26.09321', lng='27.9813'),
                           Point(lat='-26.0933', lng='27.98154'),
                           Point(lat='-26.09341', lng='27.98186')]
        self.assertEqual(points, expected_points)
