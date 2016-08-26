import unittest

from path_view.core import (
    Point,
    find_closest_point_pair,
)


class TestFindClosestPointPair(unittest.TestCase):

    def test_find_closest_point_pair(self):
        points = [Point(0, 0), Point(0, 30), Point(0, 60), Point(0, 90), Point(0, 120)]
        closest_point_pair = find_closest_point_pair(points, Point(0.0001, 45))
        self.assertEqual(closest_point_pair, (Point(0, 30), Point(0, 60)))
