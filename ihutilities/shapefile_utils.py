#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to shapefiles
"""

import shapefile

def load_shapefile_data(data_path):
    sf = shapefile.Reader(data_path)
    shapes = sf.shapes()
    file_length = len(shapes)
    return sf, file_length

def make_bbox_polygon(shp_bbox):
    bb_str = [str(x) for x in shp_bbox]
    bb_polygon = (" ".join([bb_str[0], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[1]]))
    
    polygon = "POLYGON(({}))".format(bb_polygon)

    return polygon 

def make_multipolygon(points, parts):
    bb_str = [str(x) for x in shp_bbox]
    bb_polygon = (" ".join([bb_str[0], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[1]]))
    
    polygon = "POLYGON(({}))".format(bb_polygon)

    return polygon

def make_linestring(shp_points):
    linestring = "LineString("
    for point in shp_points:
        linestring = linestring + "{} {},".format(point[0], point[1]) 
    linestring = linestring[:-1] + ")"

    return linestring

def make_point(shp_point):
    point = "POINT({} {})".format(shp_point[0], shp_point[1])
    return point


