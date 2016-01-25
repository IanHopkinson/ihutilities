#!/usr/bin/env python
# encoding: utf-8
from __future__ import unicode_literals

import csv
import logging
import operator
import os
import math

from collections import OrderedDict

def write_dictionary(filename, data, append=True):
    """This function writes a list of dictionaries to a CSV file

    Args:
       filename (str): 
            file path to the output file     
       data (list of dictionaries):
            A list of ordered dictionaries to write

    Kwargs:
       append (bool): 
            if True then data is appended to an existing file 
            if False and the file exists then the file is deleted 
       
    Returns:
       No return value

    Raises:

    Usage:
        >>> 
    """    
    keys = data[0].keys()

    newfile =  not os.path.isfile(filename)

    if not append and not newfile:
        logging.warning("Append is False, and {} exists therefore file is being deleted".format(filename))
        os.remove(filename)
        newfile = True
    elif not newfile and append:
        logging.info("Append is True, and {} exists therefore data is being appended".format(filename))
    else:
        logging.info("New file {} is being created".format(filename))

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

