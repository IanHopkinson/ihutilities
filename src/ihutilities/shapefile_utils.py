#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to shapefiles
"""

import shapefile

from matplotlib import pyplot as plt

from collections import OrderedDict

import math

# ShapeType lookup from here: https://en.wikipedia.org/wiki/Shapefile
shapetype_lookup = {
    0: "Null shape",
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
    31: "MultiPatch",
}


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

    """
    bb_str = [str(round(x, 1)) for x in shp_bbox]
    bb_polygon = (
        " ".join([bb_str[0], bb_str[1]])
        + ","
        + " ".join([bb_str[2], bb_str[1]])
        + ","
        + " ".join([bb_str[2], bb_str[3]])
        + ","
        + " ".join([bb_str[0], bb_str[3]])
        + ","
        + " ".join([bb_str[0], bb_str[1]])
    )

    polygon = "POLYGON(({}))".format(bb_polygon)
    return polygon


def make_multipolygon(points, parts, decimate_threshold=None):
    # MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))
    # print(parts, flush=True)
    # print(points, flush=True)

    prefix = "MULTIPOLYGON("
    suffix = ")"

    list_of_polygons = _convert_parts(points, parts, decimate_threshold=decimate_threshold)

    polygons = ""

    for i, points in enumerate(list_of_polygons):
        origin = points[0]
        polygon = "(("
        for i, point in enumerate(points):
            polygon = polygon + str(round(point[0], 0)) + " " + str(round(point[1], 0)) + ", "

        polygon = polygon + str(round(origin[0], 0)) + " " + str(round(origin[1], 0)) + ")),"

        polygons = polygons + polygon
        # print(polygon[:100], flush=True)

    output_polygon = prefix + polygons[:-1] + suffix

    # print(output_polygon)
    return output_polygon


def make_polygon(points, parts, decimate_threshold=None):
    # POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))
    # print(parts, flush=True)
    # print(points, flush=True)

    prefix = "POLYGON("
    suffix = ")"

    list_of_polygons = _convert_parts(points, parts, decimate_threshold=decimate_threshold)

    polygons = ""

    for i, points in enumerate(list_of_polygons):
        origin = points[0]
        polygon = "("
        for i, point in enumerate(points):
            polygon = polygon + str(round(point[0], 0)) + " " + str(round(point[1], 0)) + ", "

        polygon = polygon + str(round(origin[0], 0)) + " " + str(round(origin[1], 0)) + "),"

        polygons = polygons + polygon
        # print(polygon[:100], flush=True)

    output_polygon = prefix + polygons[:-1] + suffix

    # print(output_polygon)
    return output_polygon


def make_linestring(shp_points):
    linestring = "LineString("
    for point in shp_points:
        linestring = linestring + "{} {},".format(point[0], point[1])
    linestring = linestring[:-1] + ")"

    return linestring


def make_point(shp_point):
    point = "POINT({} {})".format(shp_point[0], shp_point[1])
    return point


def summarise_shapefile(sf, limit=9, to_screen=True):
    fieldnames = [x[0] for x in sf.fields[1:]]

    shapes = sf.shapes()
    file_length = len(shapes)

    if to_screen:
        print("\nShapefile contains {} records".format(file_length), flush=True)
        print("Fieldnames: {}\n".format(fieldnames), flush=True)
        print("First {} records:".format(limit), flush=True)

    shapetypes = set()

    # print("{} {}".format("number", fieldnames_header))
    for i, sr in enumerate(sf.iterShapeRecords()):

        # Populate from shape
        fields = [f for f in dir(sr.shape) if not f.startswith("_")]

        shapetypes.add(sr.shape.shapeType)

        content = []
        for item in sr.record:
            if isinstance(item, str):
                content.append(item)
            else:
                content.append(str(item))

        content_str = ",".join(content)
        # for field in fields:
        #     # print(field, getattr(sr.shape, field))
        #     values = getattr(sr.shape, field)
        #     if not isinstance(values, int):
        #         #print(field, values[0:9])
        #         print("{}. Length = {}".format(field, len(values)))
        #     else:
        #         print(field, values, flush=True)
        if to_screen:
            print("{}. {}".format(i + 1, content_str))
        if i >= limit:
            break

    if to_screen:
        print("\nShapefile attributes: {}".format(fields), flush=True)
    shapetypes_str = [shapetype_lookup[s] for s in shapetypes]
    if to_screen:
        print("Shapetypes found: {}\n".format(shapetypes_str), flush=True)

    return fieldnames


def plot_shapefile(sf, limit=9, bbox=False, label=None):
    _ = plt.figure(0)

    fieldnames = [x[0] for x in sf.fields[1:]]

    if label is not None:
        idx_label = fieldnames.index(label)

    for i, sr in enumerate(sf.iterShapeRecords()):
        if sr.shape.shapeType not in [5, 15]:
            print(
                "ihutilities.plot_shapefile does not currently handle shapeType {} ({})".format(
                    sr.shape.shapeType, shapetype_lookup[sr.shape.shapeType]
                ),
                flush=True,
            )
            break

        if bbox:
            list_of_polygons = _convert_bbox_to_coords(sr.shape.bbox)
        else:
            list_of_polygons = _convert_parts(
                sr.shape.points,
                sr.shape.parts,
                shapetype=sr.shape.shapeType,
                decimate_threshold=10000,
            )

        ps = []
        for polygon in list_of_polygons:
            x = []
            y = []
            for point in polygon:
                x.append(point[0])
                y.append(point[1])
            # This ensures multipolygons are all plotted the same colour
            if len(ps) == 0:
                ps = plt.plot(x, y)
            else:
                ps = plt.plot(x, y, color=ps[0].get_color())

        if label is not None:
            label_text = sr.record[idx_label]
            # print(label_text, flush=True)
            x = (sr.shape.bbox[0] + sr.shape.bbox[2]) / 2
            y = (sr.shape.bbox[1] + sr.shape.bbox[3]) / 2

            plt.text(
                x,
                y,
                label_text,
                color=ps[0].get_color(),
                verticalalignment="center",
                horizontalalignment="center",
            )

        if i > limit:
            break

    # Quick plot of bounding boxes
    # for polygon in polygon_list:
    #    x, y = convert_db_polygon_to_coords(polygon["bounding_box"])
    #    plt.plot(x, y, 'k')

    plt.axes().set_aspect("equal", "datalim")
    plt.xlabel("eastings")
    plt.ylabel("northings")
    plt.show()


def _convert_bbox_to_coords(bb):
    coords = [[[bb[0], bb[1]], [bb[2], bb[1]], [bb[2], bb[3]], [bb[0], bb[3]], [bb[0], bb[1]]]]

    return coords


def _convert_parts(points, parts, shapetype=15, decimate_threshold=None):
    # Takes a points array and a parts array and returns a list of lists of x,y coordinates

    list_of_parts = []
    for j in range(len(parts)):
        start_index = parts[j]
        try:
            end_index = parts[j + 1]
        except IndexError:
            end_index = len(points)
        chunk = []

        part_length = end_index - start_index
        if decimate_threshold is not None and part_length > decimate_threshold:
            ratio = math.ceil(part_length / decimate_threshold)
            print(
                "Part size = {}, exceeds decimate_threshold = {}".format(
                    part_length, decimate_threshold
                ),
                flush=True,
            )
            print("Using ratio = {}".format(ratio), flush=True)
            for i, point in enumerate(points[start_index:end_index]):
                if i % ratio == 0:
                    chunk.append(point)
                # chunk.append(points[end_index - 1])
        else:
            for point in points[start_index:end_index]:
                chunk.append(point)

        list_of_parts.append(chunk)

    return list_of_parts
