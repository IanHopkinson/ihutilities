#!/usr/bin/env python
# encoding: utf-8

from nose.tools import assert_equal
from ihutilities import make_bbox_polygon, make_multipolygon

def test_make_bbox_polygon():
    test_coords = [489.0, 212.0, 527.0, 270.0]

    polygon = make_bbox_polygon(test_coords)

    assert_equal(polygon, "POLYGON((489.0 212.0,527.0 212.0,527.0 270.0,489.0 270.0,489.0 212.0))")

def test_make_multipolygon():
    test_coords = [[0, 0], [1, 1], [0, 2]]
    parts = [0]

    polygon = make_multipolygon(test_coords, parts)

    assert_equal(polygon, "MULTIPOLYGON(((0 0, 1 1, 0 2, 0 0)))")