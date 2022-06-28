#!/usr/bin/env python
# encoding: utf-8

import unittest
from ihutilities import make_bbox_polygon, make_multipolygon


class ShapefileTests(unittest.TestCase):
    def test_make_bbox_polygon(self):
        test_coords = [489.0, 212.0, 527.0, 270.0]

        polygon = make_bbox_polygon(test_coords)

        self.assertEqual(
            polygon, "POLYGON((489.0 212.0,527.0 212.0,527.0 270.0,489.0 270.0,489.0 212.0))"
        )

    def test_make_multipolygon(self):
        test_coords = [[0, 0], [1, 1], [0, 2]]
        parts = [0]

        polygon = make_multipolygon(test_coords, parts)

        self.assertEqual(polygon, "MULTIPOLYGON(((0 0, 1 1, 0 2, 0 0)))")
