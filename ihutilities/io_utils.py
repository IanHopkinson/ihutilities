#!/usr/bin/env python
# encoding: utf-8
from __future__ import unicode_literals

import csv
import operator
import os
import math

from collections import OrderedDict

def write_dictionary(filename, data):    
    keys = data[0].keys()

    newfile =  not os.path.isfile(filename)

    with open(filename, 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, lineterminator='\n')
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(data)

def pretty_print_dict(dictionary):
    # print("Feature names: {}\n".format(feature_names))
    WIDTH = 160
    # find longest feature_name
    max_width = max([len(key) for key in dictionary.keys()]) + 2
    # find out how many of longest feature name fit in 80 characters
    n_columns = math.floor(WIDTH/(max_width + 7))
    # Build format string
    fmt = "%{}s:%3d".format(max_width)
    # feed feature_names into format string
    report = ''
    i = 1
    for key, value in dictionary.items():
        report = report + fmt % (key,value)
        if (i % n_columns) == 0:
            report = report + "\n"
        i = i + 1

    print(report)
    return report

def sort_dict_by_value(unordered_dict):
    sorted_dict = sorted(unordered_dict.items(), key=operator.itemgetter(1))
    return OrderedDict(sorted_dict)

