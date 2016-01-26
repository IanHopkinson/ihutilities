#!/usr/bin/env python
# encoding: utf-8

import csv

from collections import Counter

def survey_csv(file_path, line_limit=1000):
    # Count the lines in a CSV
    # List the fields in a CSV (so they can be copy pasted)
    # Count the empty values for each field
    # Count the contents of each field
    # Guess the type of a field
    empty_count = Counter()
    with open(file_path, encoding='utf-8-sig') as f:
        rows = csv.reader(f)
        headers = next(rows)

    with open(file_path, encoding='utf-8-sig') as f:
        rows = csv.DictReader(f)
        for i, row in enumerate(rows):
            for field in headers:
                if len(row[field]) == 0:
                    empty_count[field] += 1
            if i > line_limit:
                break

    line_count = i + 1

    print("**Survey results for {}**".format(file_path))
    print("Number of lines: {}".format(line_count))
    print("Fields:")
    print("Name, filled percentage")
    for field in headers:
        print("{0: <30}: {1:.3f}%".format(field, 100.0 * (1.0 - empty_count[field]/line_count)))

if __name__ == "__main__":
    survey_csv()
    