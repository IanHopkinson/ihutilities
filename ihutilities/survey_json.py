#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import decimal
import requests
import sys
import time

import dateutil.parser as parser

from collections import Counter
from pprint import pprint


def survey_json(file_path, line_limit=1000, encoding=None):
    if encoding is None:
        encoding = 'utf-8-sig'
    print("\nStarting surveying {} at {}".format(file_path, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), flush=True)
    t0 = time.time()
    # Count the lines in a CSV
    # List the fields in a CSV (so they can be copy pasted)
    # Count the empty values for each field
    # Count the contents of each field
    # Guess the type of a field
    line_count = 0
    report_size = 1000000

    filled_count = Counter()

    json_data = {}
    if file_path.startswith("http"):
        # try:
        r = requests.get(file_path)
        json_data = r.json()
        #except:
        #r = None
    else:
        with open(file_path, encoding=encoding) as f:
            json_data = json.load(f)

    flattened_data = unwind_nested_dictionary(json_data, None)

    pprint(flattened_data)

        # for el in flattened_data.keys():
        #     print(key, flattened_data, flush=True)

        # print(json_data.keys(), flush=True)

    # with open(file_path, encoding=encoding) as f:
    #     rows = csv.reader(f)
    #     headers = next(rows)

    # field_set = {}
    # for field in headers:
    #     field_set[field] = set()


    # with open(file_path, encoding=encoding) as f:
    #     rows = csv.DictReader(f)
    #     try:
    #         for i, row in enumerate(rows):
    #             line_count += 1
    #             # Filling percentage
    #             for field in headers:
    #                 if len(row[field]) != 0:
    #                     filled_count[field] += 1

    #                 # Categorical enumeration
    #                 field_set[field].add(row[field])

    #             if i > line_limit:
    #                 break

    #             if (line_count % report_size) == 0:
    #                     print("\nSurvey interim results")
    #                     print("==============")
    #                     t1 = time.time()
    #                     elapsed = t1 - t0
    #                     print_report(file_path, elapsed, line_limit, line_count, headers, filled_count)
    #     except Exception as ex:
    #         print("Encountered exception '{}' at line_count = {}".format(ex, line_count))
    #         print("Aborting")


    # print("\nSurvey final results")
    # print("==============")
    # t1 = time.time()
    # elapsed = t1 - t0
    # line_count = line_count - 2
    # print_report(file_path, elapsed, line_limit, line_count, headers, filled_count, field_set)

def print_report(file_path, elapsed, line_limit, line_count, headers, filled_count, field_set):

    print("\nFile path: {}".format(file_path), flush=True)
    print("Time to survey: {0:.2f} seconds".format(elapsed), flush=True)
    if line_count <= line_limit:
        print("Number of lines: {}".format(line_count), flush=True)
    else:
        print("Line limit set to: {}".format(line_limit), flush=True)
    print("Number of fields: {}".format(len(headers)), flush=True)
    print("\nField list:", flush=True)
    print("i, Name, filled_count, distinct_count, field_type, minimum, maximum", flush=True)
    for i, field in enumerate(headers, start=1):
        # filled_percentage = 100.0 * (1.0 - empty_count[field]/line_count)
        distinct_count = len(field_set[field])
        type_ = type_sniff(field_set, field)
        if type_ == "IntegerType":
            convfunc = int
        elif type_ == "DecimalType":
            convfunc = decimal.Decimal
        elif type_ == "DateType":
            convfunc = parser.parse
        else:
            convfunc = str
        array = []
        for x in field_set[field]:
            if x not in ["", None]:
                try:
                    array.append(convfunc(x))
                except:
                    pass
        #array = [convfunc(x) for x in field_set[field] if x not in ["", None]]
        if len(array) != 0: 
            minimum = min(array)
            maximum = max(array)
        else:
            minimum = None
            maximum = None
        print("{0: <3}, {1: <30}, {2: <10}, {3: <10}, {4:}, {5:}, {6:}".format(i,field, filled_count[field], distinct_count, type_, minimum, maximum), flush=True)

    return

def type_sniff(field_set, field):
    data_set = field_set[field]
    types = ["StringType", "DecimalType", "IntegerType", "DateType"]

    type_scores = Counter()
    fail_scores = Counter()

    for item in data_set:
        # If item is none then continue
        if item in ("", None):
            continue
        # Check for date
        try:
            date_ = parser.parse(item)
            type_scores["DateType"] += 2
        except:
            fail_scores["DateType"] += 2

        # check for integer
        try:
            value = int(item)
            type_scores["IntegerType"] += 3
        except:
            fail_scores["IntegerType"] += 3

        # check for float/decimal
        try:
            value = decimal.Decimal(item)
            type_scores["FloatType"] += 2
        except:
            fail_scores["FloatType"] += 2

        # check for string (string is the fallback)
        type_scores["StringType"] += 1

    try:
        type_ = type_scores.most_common(1)[0][0] 
        #type_ = type_scores.most_common(5)
        anti_type = fail_scores.most_common(5)
    except:
        type_ = "NoneType"
        anti_type = "NoneType"

    return type_ #, anti_type

def unwind_nested_dictionary(source, destination, root=""):
    if destination is None:
        destination = {}

    #print(source, flush=True)
    # if not isinstance(source, dict):
    #     print(type(source), source, flush=True)
    #     return destination

    
    for k, v in source.items():
        #print(k,v, type(v))
        if isinstance(v, dict):
            unwind_nested_dictionary(v, destination, root=root+"-"+k)
        elif isinstance(v, list) and len(v) != 0:
            if isinstance(v[0], dict):
                n = len(v)
                unwind_nested_dictionary(v[0], destination, root=root+"-"+k+"[1of{}]".format(n))
            elif isinstance(v[0], str):
                destination[root + "-" + k] = "|".join(v)
        else:
        # print("{0} : {1}".format(k, v), flush=True)
            destination[root + "-" + k] = v

    return destination

if __name__ == "__main__":
    arg = sys.argv[1:]
    encoding = None
    if len(arg) == 0:
        print("Available commandlines:")
        print("survey_json.py file_path")
        print("survey_json.py file_path [line_limit = {integer or all}] [encoding]")
        print("\n Default file encoding is utf-8-sig, cp1252 is Windows default so worth a try, and iso-8859-1 encodes all bytes so at least it won't barf")
        #sys.exit()
        # file_path = "https://api.bankofscotland.co.uk/open-banking/v1.2/branches"
        # file_path = "https://openapi.bankofireland.com/open-banking/v1.2/branches" - doesn't work because of ssl problems
        file_path = "https://atlas.api.barclays/open-banking/v1.3/branches"
        line_limit = 1000

    elif len(arg) == 1:
        file_path = arg[0]
        line_limit = 1000
    elif len(arg) == 2:
        file_path = arg[0]
        if arg[1] == "all":
            line_limit = float("inf")
        else:
            line_limit = int(arg[1])
    elif len(arg) == 3:
        file_path = arg[0]
        if arg[1] == "all":
            line_limit = float("inf")
        else:
            line_limit = int(arg[1])
        encoding = arg[2]

    survey_json(file_path, line_limit, encoding)
    