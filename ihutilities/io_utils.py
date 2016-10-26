#!/usr/bin/env python
# encoding: utf-8
from __future__ import unicode_literals

import csv
import fnmatch
import hashlib
import io
import logging
import operator
import glob
import os
import math
import subprocess
import zipfile

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

def calculate_file_sha(filepath):
    file_sha = hashlib.sha1()

    fh = get_a_file_handle(filepath, mode="rb")

    if ".zip" not in filepath:
        file_size = os.path.getsize(filepath)
    else:
        zip_path, name_in_zip = split_zipfile_path(filepath)
        if len(name_in_zip) != 0:
            zf = zipfile.ZipFile(zip_path)
            file_size = zf.getinfo(name_in_zip).file_size
        else: # if no name in zip is specified then calculate the sha of the zip file as a whole
            file_size = os.path.getsize(filepath)
            fh = open(filepath, "rb")   

    
    #This magic should make our sha match the git sha
    file_sha.update("blob {:d}\0".format(file_size).encode("utf-8"))

    #with open(filepath, "rb") as f:
    with fh:
        for chunk in iter(lambda: fh.read(4096), b""):
            file_sha.update(chunk)

    return file_sha.hexdigest()

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

def get_a_file_handle(file_path, encoding="utf-8-sig", mode="rU", zip_guess=True):
    """This function returns a file handle, even if a file is within a zip

    Args:
       file_path (str): 
            file path to the output file

    Kwargs:
       append (bool): 
            if True then data is appended to an existing file 
            if False and the file exists then the file is deleted 
       
    Returns:
       a file handler

    Raises:

    Usage:
        >>> 
    """
    # If we have a straightforward file then return that
    if ".zip" not in file_path:
        if mode == "rU":
            fh = open(file_path, encoding=encoding, mode=mode)
        else: # This is what we do for binary files
            fh = open(file_path, mode=mode)
    else:
        zip_path, name_in_zip = split_zipfile_path(file_path)
        zf = zipfile.ZipFile(zip_path)
        namelist = zf.namelist()

        if len(name_in_zip) == 0:
            try:
                cf = zf.open(namelist[0], "rU")
            except (NotImplementedError, OSError):
                raise
            if mode == "rU":
                fh = io.TextIOWrapper(io.BytesIO(cf.read()), encoding=encoding)
            else:
                fh = io.BytesIO(cf.read())
        else:
            for name in namelist:
                if fnmatch.fnmatch(name, name_in_zip):
                    try:
                        cf = zf.open(name, "rU")
                    except (NotImplementedError, OSError):
                        raise
                    if mode == "rU":
                        fh = io.TextIOWrapper(io.BytesIO(cf.read()), encoding=encoding)
                    else:
                        fh = io.BytesIO(cf.read())               
    
    return fh

def split_zipfile_path(zipfile_path):
    parts = zipfile_path.split(".zip")
    zip_path = parts[0] + ".zip"
    if len(parts) != 0 and len(parts) == 2:
        name_in_zip = parts[1][1:]
    else:
        name_in_zip = ""

    return zip_path, name_in_zip
