#!/usr/bin/env python
# encoding: utf-8

from __future__ import unicode_literals

import csv
import fnmatch
import hashlib
import io
import logging
import math
import operator
import os
import shutil
import time
import zipfile
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


def write_dictionary(filename: str, data: List[Dict[str,Any]], append:Optional[bool]=True, 
                     delimiter:Optional[str]=",", quoting:Optional[int]=csv.QUOTE_MINIMAL,
                     encoding:Optional[str]="utf-8") -> None:
    """This function writes a list of dictionaries to a CSV file

    Args:
       filename (str): 
            file path to the output file     
       data (list of dictionaries):
            A list of ordered dictionaries to write

    Keyword args:
       append (bool): 
            if True then data is appended to an existing file 
            if False and the file exists then the file is deleted 
       delimiter (str):
            Delimiter character as per dictwriter interface
    Returns:
       No return value

    Example:
        >>> 
    """    
    keys = data[0].keys()

    newfile =  not os.path.isfile(filename)

    if not append and not newfile:
        logging.warning("Append is False, and {} exists therefore file is being deleted".format(filename))
        os.remove(filename)
        newfile = True
    elif not newfile and append:
        logger.info("Append is True, and {} exists therefore data is being appended".format(filename))
    else:
        logging.info("New file {} is being created".format(filename))

    with open(filename, 'a', encoding=encoding, errors="ignore") as output_file:
        dict_writer = csv.DictWriter(output_file, keys, lineterminator='\n', delimiter=delimiter, quoting=quoting, escapechar="\\")
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(data)

def calculate_file_sha(filepath:str, encoding="utf-8-sig"):
    file_sha = hashlib.sha1()

    # Switched this to get sha calculation for files within zip files working
    #fh = file_handle_or_none(filepath, encoding=None, mode="rb")
    fh = get_a_file_handle(filepath, encoding=encoding, mode="rb", zip_guess=True)

    if fh is None:
        return None

    if ".zip" not in filepath.lower():
        file_size = os.path.getsize(filepath)
    else:
        zip_path, name_in_zip = split_zipfile_path(filepath)
        if (name_in_zip is not None) and (zip_path is not None) and len(name_in_zip) != 0 and "*" not in name_in_zip:
            zf = zipfile.ZipFile(zip_path)
            file_size = zf.getinfo(name_in_zip).file_size
        else: # if no name in zip is specified then calculate the sha of the zip file as a whole
            file_size = os.path.getsize(zip_path)
            fh = open(zip_path, "rb")   

    
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

def get_a_file_handle(file_path:str, encoding:Optional[str] ="utf-8-sig", mode:Optional[str]="r", zip_guess:Optional[bool]=True):
    """This function returns a file handle, even if a file is within a zip

    Args:
        file_path (str): 
            file path which may point to a file inside a zip

    Keyword args:
        encoding (str): 
            character encoding of the taregt file
        mode (str): 
            mode to use for opening file
        zip_guess (bool): 
            if True then we try to guess whether the file is a zip   
       
    Returns:
       a file handler

    Example:
        >>> 
    """
    # If we have a straightforward file then return that
    fh = None 
    if file_path is None:
        return fh

    if ".zip" not in file_path.lower():
        if mode == "r":
            fh = file_handle_or_none(file_path, encoding=encoding, mode=mode)
        else: # This is what we do for binary files, no encoding permitted here
            fh = file_handle_or_none(file_path, encoding=None, mode=mode)
    else:
        zip_path, name_in_zip = split_zipfile_path(file_path)
        zf = zipfile.ZipFile(zip_path)
        namelist = zf.namelist()

        if len(name_in_zip) == 0:
            try:
                cf = zf.open(namelist[0], "r")
            except (NotImplementedError, OSError):
                raise
            if mode == "r":
                fh = io.TextIOWrapper(io.BytesIO(cf.read()), encoding=encoding)
            else:
                fh = io.BytesIO(cf.read())
        else:
            for name in namelist:
                if fnmatch.fnmatch(name, name_in_zip):
                    try:
                        cf = zf.open(name, "r")
                    except (NotImplementedError, OSError):
                        raise
                    if mode == "r":
                        fh = io.TextIOWrapper(io.BytesIO(cf.read()), encoding=encoding)
                    else:
                        fh = io.BytesIO(cf.read())               
    
    return fh

def file_handle_or_none(file_path, encoding="utf-8-sig", mode="r") -> Any:
    try:
        if encoding is not None:
            fh = open(file_path, encoding=encoding, mode=mode)
        else:
            fh = open(file_path, mode=mode)
    except FileNotFoundError:
        fh = None
    return fh

def split_zipfile_path(zipfile_path:str) -> Tuple[Any, Any]:
    if zipfile_path is None:
        return None, None
    if ".zip" not in zipfile_path.lower():
        zip_path = zipfile_path
        name_in_zip = ""
        return zip_path, name_in_zip

    if ".zip" in zipfile_path:
        parts = zipfile_path.split(".zip")
        zip_path = parts[0] + ".zip"
    else:
        parts = zipfile_path.split(".ZIP")
        zip_path = parts[0] + ".ZIP"

    if len(parts) != 0 and len(parts) == 2:
        name_in_zip = parts[1][1:]
    else:
        name_in_zip = ""

    return zip_path, name_in_zip

def download_file_from_url(url:str, local_filepath:str) -> str:
    """
    A function to download and save a file from a url
    
    Args:
        url (str): url of file to download
        local_filepath (str): local file path to which to save
    
    Returns:
        local_filepath (str): the path to which the file was saved
    """
    t0 = time.time()
    logger.info("Downloading file from {}, saving to {}".format(url, local_filepath))

    if not os.path.isdir(os.path.dirname(local_filepath)):
        os.makedirs(os.path.dirname(local_filepath))

    if os.path.isfile(local_filepath):
        logger.warning("Local file {} already exists, delete to download again".format(local_filepath))
        return local_filepath

    # NOTE the stream=True parameter
    try:
        r = requests.get(url, stream=True, timeout=3.5)
    except: 
        time.sleep(5)
        logger.warning("Connection to {} failed on first try, making second attempt".format(url))
        r = requests.get(url, stream=True, timeout=3.5)
    
    chunk_count = 0
    tmp_path = local_filepath + "_tmp"
    with open(tmp_path, 'wb') as f:
        for i, chunk in enumerate(r.iter_content(chunk_size=1024)): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                chunk_count += 1
                #f.flush() commented by recommendation from J.F.Sebastian
                if (i % 1000) == 0:
                    print('.', end='', flush=True)
    print("", flush=True)
    t1 = time.time()
    shutil.copy(tmp_path, local_filepath)
    os.remove(tmp_path)

    logger.info("Download took {:.2f}seconds for {:.2f}mb\n".format(t1 - t0, chunk_count / 1024))

    return local_filepath

def colour_text(text:str, colour: Optional[str]="red") -> str:
    """
    Decorate a text string with ANSI escape codes for coloured text in bash-like shells
    
    Args:
        text (str): A list of addresses
    
    Keyword arguments:
        colour (str): the required colour (currently supported: red, green, blue, cyan, white, yellow, magenta, grey, black)
    
    Returns:
        coloured_text (str): a dictionary containing the answers
        
    """
    # Long list of colours/
    # https://stackoverflow.com/questions/15580303/python-output-complex-line-with-floats-colored-by-value
    #https://github.com/ryanoasis/public-bash-scripts/blob/master/unix-color-codes.sh
    prefix_set = {}

    prefix_set["red"] = '\033[91m'
    prefix_set["green"] = '\033[92m'
    prefix_set["blue"] = '\033[94m'
    prefix_set["cyan"] = '\033[96m'
    prefix_set["white"] = '\033[97m'
    prefix_set["yellow"] = '\033[93m'
    prefix_set["magenta"] = '\033[95m'
    prefix_set["grey"] = '\033[90m'
    prefix_set["black"] = '\033[30m'
    prefix_set["default"] = '\033[99m'

    prefix_set["light_red"] = '\033[31m'
    prefix_set["light_green"] = '\033[32m'
    prefix_set["light_yellow"] = '\033[33m'
    prefix_set["light_blue"] = '\033[34m'
    prefix_set["light_magenta"] = '\033[35m'
    prefix_set["light_cyan"] = '\033[36m'
    prefix_set["light_white"] = '\033[37m'

    prefix = prefix_set.get(colour, prefix_set["default"])
    
    suffix = "\033[0m"

    coloured_text = prefix + text + suffix

    return coloured_text
