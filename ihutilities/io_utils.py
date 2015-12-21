#!/usr/bin/env python
# encoding: utf-8
from __future__ import unicode_literals

import csv
import os

def write_dictionary(filename, data):    
    keys = data[0].keys()

    newfile =  not os.path.isfile(filename)

    with open(filename, 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, lineterminator='\n')
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(data)
