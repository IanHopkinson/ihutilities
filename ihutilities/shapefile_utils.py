#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to shapefiles
"""

import shapefile

def load_shapefile_data(data_path):
    """This function loads a shapefile into a reader

    Args:
       data_path (str): 
            A file path to a shapefile

    Returns:
       sf:
            A shapefile reader object over which you can iterate
       file_length (integer):
            the number of shapes in the file

    Raises:

    Usage:
        >>> 
    """
    sf = shapefile.Reader(data_path)
    shapes = sf.shapes()
    file_length = len(shapes)
    return sf, file_length

def make_bbox_polygon(shp_bbox):
    """This function converts a shapefile array into a POLYGON string to load into MySQL/MariaDB

    Args:
       shp_bbox (shapefile array): 
            An array containing upper left and lower right coordinates of a bounding box

    Returns:
       polygon (str):

    Raises:

    Usage:
        >>> 
    """
    bb_str = [str(x) for x in shp_bbox]
    bb_polygon = (" ".join([bb_str[0], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[1]]))
    
    polygon = "POLYGON(({}))".format(bb_polygon)
    return polygon 

def make_multipolygon(points, parts):
    bb_str = [str(x) for x in points]
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


