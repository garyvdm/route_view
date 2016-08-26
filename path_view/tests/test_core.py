import unittest

from path_view.core import (
    Point,
    find_closest_point_pair,
    iter_points_with_minimal_spacing,
)


class TestHelpers(unittest.TestCase):

    def test_find_closest_point_pair(self):
        points = [Point(0, 0), Point(0, 30), Point(0, 60), Point(0, 90), Point(0, 120)]
        closest_point_pair = find_closest_point_pair(points, Point(0.0001, 45))
        self.assertEqual(closest_point_pair, (Point(0, 30), Point(0, 60)))

    def test_iter_points_with_minimal_spacing(self):
        points = [Point(0, 0), Point(0, 0.1), Point(0.1, 0.1)]
        points_with_minimal_spacing = list(iter_points_with_minimal_spacing(points, spacing=4000))
        # bit lazy to do asserts on the acatual values returned here. Just check the the correct number of items
        # are returned
        for point in points_with_minimal_spacing:
            print(point)
        self.assertEqual(len(points_with_minimal_spacing), 5)
