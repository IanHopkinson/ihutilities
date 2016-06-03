#!/usr/bin/env python
# encoding: utf-8

from nose.tools import assert_equal
from ihutilities import make_bbox_polygon

def test_make_bbox_polygon():
    test_coords = [489.0, 212.0, 527.0, 270.0]

    polygon = make_bbox_polygon(test_coords)

    assert_equal(polygon, "POLYGON((489.0 212.0,527.0 212.0,527.0 270.0,489.0 270.0,489.0 212.0))")