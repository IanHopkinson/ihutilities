#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to shapefiles
"""

import shapefile

from collections import OrderedDict

# ShapeType lookup from here: https://en.wikipedia.org/wiki/Shapefile
shapetype_lookup = {0: "Null shape",
                    1: "Point",
                    3: "Polyline",
                    5: "Polygon",
                    8: "Multipoint",
                    11: "PointZ",
                    13: "PolylineZ",
                    15: "PolygonZ",
                    18: "MultipointZ",
                    21: "PointM",
                    23: "PolylineM",
                    25: "PolygonM",
                    28: "MultipointM",
                    31: "MultiPatch"}

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
    bb_str = [str(round(x,1)) for x in shp_bbox]
    bb_polygon = (" ".join([bb_str[0], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[1]]) + "," +
                  " ".join([bb_str[2], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[3]]) + "," +
                  " ".join([bb_str[0], bb_str[1]]))
    
    polygon = "POLYGON(({}))".format(bb_polygon)
    return polygon 

def make_multipolygon(points, parts):
    # MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))
    #print(parts, flush=True)
    #print(points, flush=True)

    polygon = "MULTIPOLYGON((("

    origin = points[0]
    if len(parts) == 1:
        for i, point in enumerate(points):
            polygon = polygon + str(round(point[0], 1)) + " " + str(round(point[1], 1)) + ", "
        polygon = polygon + str(round(origin[0], 1)) + " " + str(round(origin[1], 1)) + ")))"
        #print(polygon[:100], flush=True)
    else:
        #print("{} parts = {}".format(len(parts), parts), flush=True)
        #print(len(points), flush=True)
        polygon = polygon = "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)))"

    #

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

def summarise_shapefile(sf, limit=9):
    fieldnames = [x[0] for x in sf.fields[1:]]

    shapes = sf.shapes()
    file_length = len(shapes)

    print("\nShapefile contains {} records".format(file_length), flush=True)
    print("Fieldnames: {}\n".format(fieldnames), flush=True)
    print("First {} records:".format(limit), flush=True)

    shapetypes = set()

    fieldnames_header = ",".join(fieldnames)

    #print("{} {}".format("number", fieldnames_header))
    for i, sr in enumerate(sf.iterShapeRecords()):

        # Populate from shape
        fields = [f for f in dir(sr.shape) if not f.startswith("_")]

        shapetypes.add(sr.shape.shapeType)

        data_dict = OrderedDict(zip(fieldnames, sr.record))

        content_str = ",".join(sr.record)
        # for field in fields:
        #     # print(field, getattr(sr.shape, field))
        #     values = getattr(sr.shape, field)
        #     if not isinstance(values, int):
        #         #print(field, values[0:9])
        #         print("{}. Length = {}".format(field, len(values)))
        #     else:
        #         print(field, values, flush=True)
        print("{}. {}".format(i + 1, content_str))
        if i >= limit: 
            break


    print("\nShapefile attributes: {}".format(fields), flush=True)
    shapetypes_str = [shapetype_lookup[s] for s in shapetypes]
    print("Shapetypes found: {}\n".format(shapetypes_str), flush=True)


