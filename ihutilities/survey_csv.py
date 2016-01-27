#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import sys
import time

from collections import Counter

def survey_csv(file_path, line_limit=1000):
    print("\nStarting surveying {} at {}".format(file_path, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), flush=True)
    t0 = time.time()
    # Count the lines in a CSV
    # List the fields in a CSV (so they can be copy pasted)
    # Count the empty values for each field
    # Count the contents of each field
    # Guess the type of a field
    line_count = 0
    report_size = 1000000

    empty_count = Counter()
    with open(file_path, encoding='utf-8-sig') as f:
        rows = csv.reader(f)
        headers = next(rows)

    with open(file_path, encoding='utf-8-sig') as f:
        rows = csv.DictReader(f)
        for i, row in enumerate(rows):
            line_count += 1
            for field in headers:
                if len(row[field]) == 0:
                    empty_count[field] += 1
            if i > line_limit:
                break

            if (line_count % report_size) == 0:
                    print("\nSurvey interim results")
                    print("==============")
                    t1 = time.time()
                    elapsed = t1 - t0
                    print_report(file_path, elapsed, line_limit, line_count, headers, empty_count)               
    print("\nSurvey final results")
    print("==============")
    t1 = time.time()
    elapsed = t1 - t0
    print_report(file_path, elapsed, line_limit, line_count, headers, empty_count)

def print_report(file_path, elapsed, line_limit, line_count, headers, empty_count):

    print("\nFile path: {}".format(file_path), flush=True)
    print("Time to survey: {0:.2f} seconds".format(elapsed), flush=True)
    if line_limit == float("inf"):
        print("Number of lines: {}".format(line_count), flush=True)
    else:
        print("Line limit set to: {}".format(line_limit), flush=True)
    print("Number of fields: {}".format(len(headers)), flush=True)
    print("\nField list:", flush=True)
    print("Name, filled percentage", flush=True)
    for field in headers:
        print("{0: <30}: {1:.3f}%".format(field, 100.0 * (1.0 - empty_count[field]/line_count)), flush=True)

if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) == 0:
        print("Available commandlines:")
        print("survey_csv.py file_path")
        print("survey_csv.py file_path [line_limit = {integer or all}]")
        sys.exit()
    elif len(arg) == 1:
        file_path = arg[0]
        line_limit = 1000
    else:
        file_path = arg[0]
        if arg[1] == "all":
            line_limit = float("inf")
        else:
            line_limit = int(arg[1])


    survey_csv(file_path, line_limit)
    