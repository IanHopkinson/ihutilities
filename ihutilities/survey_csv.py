#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import sys
import time

from collections import Counter

def survey_csv(file_path, line_limit=1000, encoding=None):
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
    
    with open(file_path, encoding=encoding) as f:
        rows = csv.reader(f)
        headers = next(rows)

    field_set = {}
    for field in headers:
        field_set[field] = set()


    with open(file_path, encoding=encoding) as f:
        rows = csv.DictReader(f)
        try:
            for i, row in enumerate(rows):
                line_count += 1
                # Filling percentage
                for field in headers:
                    if len(row[field]) != 0:
                        filled_count[field] += 1

                    # Categorical enumeration
                    field_set[field].add(row[field])

                if i > line_limit:
                    break

                if (line_count % report_size) == 0:
                        print("\nSurvey interim results")
                        print("==============")
                        t1 = time.time()
                        elapsed = t1 - t0
                        print_report(file_path, elapsed, line_limit, line_count, headers, filled_count)
        except Exception as ex:
            print("Encountered exception '{}' at line_count = {}".format(ex, line_count))
            print("Aborting")


    print("\nSurvey final results")
    print("==============")
    t1 = time.time()
    elapsed = t1 - t0
    line_count = line_count - 2
    print_report(file_path, elapsed, line_limit, line_count, headers, filled_count, field_set)

def print_report(file_path, elapsed, line_limit, line_count, headers, filled_count, field_set):

    print("\nFile path: {}".format(file_path), flush=True)
    print("Time to survey: {0:.2f} seconds".format(elapsed), flush=True)
    if line_count <= line_limit:
        print("Number of lines: {}".format(line_count), flush=True)
    else:
        print("Line limit set to: {}".format(line_limit), flush=True)
    print("Number of fields: {}".format(len(headers)), flush=True)
    print("\nField list:", flush=True)
    print("i, Name, filled_count, distinct_count", flush=True)
    for i, field in enumerate(headers, start=1):
        # filled_percentage = 100.0 * (1.0 - empty_count[field]/line_count)
        distinct_count = len(field_set[field])
        if len(field_set[field]) < 10:
            values = "|".join(list(field_set[field]))
        else:
            values = "|".join(list(field_set[field])[0:10]) + "..."
        print("{0: <3}, {1: <30}, {2: <10}, {3: <10} ".format(i,field, filled_count[field], distinct_count), flush=True)

    return

if __name__ == "__main__":
    arg = sys.argv[1:]
    encoding = None
    if len(arg) == 0:
        print("Available commandlines:")
        print("survey_csv.py file_path")
        print("survey_csv.py file_path [line_limit = {integer or all}] [encoding]")
        print("\n Default file encoding is utf-8-sig, cp1252 is Windows default so worth a try, and iso-8859-1 encodes all bytes so at least it won't barf")
        sys.exit()
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

    survey_csv(file_path, line_limit, encoding)
    