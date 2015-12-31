#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import os
import sqlite3
import sys
import time

from collections import OrderedDict
from ihutilities.db_utils import configure_db, write_to_db

# This dictionary has field names and field types. It should be reuseable between the configure_db and 
# write_to_db functions

def do_etl(db_fields, db_file_path, data_path, data_field_lookup, mode="production", headers=True):
    print("Starting ETL to database at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    # Scan parameters
    if mode == "production":
        test_line_limit = float('inf') # float('inf')
        chunk_size = 10000 # 10000
        report_size = 10000 # 10000
    elif mode == "test":
        test_line_limit = 10000 # float('inf')
        chunk_size = 1000 # 10000
        report_size = 1000 # 10000
    else:
        print("Mode should be either 'test' or 'production', mode supplied was '{}'".format(mode))
        sys.exit()

    
    print("Measuring length of input file...")
    file_length = report_input_length(data_path, test_line_limit)

    configure_db(db_file_path, db_fields, force=True)

    # Get on with the main business
    t0 = time.time()
    t_last = t0
    data = []
    line_count = 0
    lines_dropped = 0

    primary_key_set = set()
    duplicate_primary_keys = set()
    primary_key = get_primary_key_from_db_fields(db_fields)

    with open(data_path) as f:
        if headers:
            rows = csv.DictReader(f)
        else:
            rows = csv.reader(f)
        # Loop over input rows
        for i, row in enumerate(rows):
            new_row = OrderedDict([(x,None) for x in db_fields.keys()]) 
            # zip input row into output row
            for output_key in new_row.keys():
                # This inserts blank fields
                if data_field_lookup[output_key] is not None:
                    new_row[output_key] = row[data_field_lookup[output_key]]
                # If we have a field called ID as Primary Key and there is no lookup
                # for it we assume it is a synthetic key and put in an autoincrement value
                if primary_key == "ID" and data_field_lookup["ID"] is None:
                    new_row[primary_key] = i
            # Decide whether or not to write new_row
            if new_row[primary_key] not in primary_key_set:
                line_count += 1
                data.append(([x for x in new_row.values()]))
                primary_key_set.add(new_row[primary_key])
            else:
                #print("UPRN is a duplicate: {}".format(new_row["UPRN"]))
                duplicate_primary_keys.add(new_row[primary_key])
                lines_dropped += 1
                #print("UPRN = {} has already been seen".format(row[0])) 

            # Write an interim report
            if (line_count % report_size) ==0:
                est_completion_time = ((time.time() - t0) / line_count) * (min(file_length, test_line_limit) - line_count)
                completion_str = (datetime.datetime.now() + datetime.timedelta(seconds=est_completion_time)).strftime("%Y-%m-%d %H:%M:%S")
                print("Wrote {}/{} at ({}). Estimated completion time: {}".format(
                        line_count, 
                        file_length,
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        completion_str), flush=True)
                t_last = time.time()

            # Write a chunk to the database            
            if (line_count % chunk_size) == 0:
                write_to_db(data, db_file_path, db_fields)
                data = []

            # Break if we have reached test_line_limit
            if i > test_line_limit:
                break

    # Final write to database
    write_to_db(data, db_file_path, db_fields)

    # Write a final report
    t1 = time.time()
    elapsed = t1 - t0
    print("\nWrote a total {} lines to the database in {:.2f}s".format(line_count, elapsed), flush=True)
    print("Dropped {} lines because they contained duplicate primary key ({})".format(lines_dropped, primary_key))

def get_primary_key_from_db_fields(db_fields):
    primary_key = ""
    for key, value in db_fields.items():
        if value.upper().endswith("PRIMARY KEY"):
            primary_key = key
    return primary_key

def report_input_length(data_path, test_line_limit):
    t0 = time.time()
    with open(data_path) as f:
        rows = csv.reader(f)
        file_length = sum(1 for row in rows) - 1 #Take off the header line
        data_file = os.path.basename(data_path)
        print("Importing '{}'. {} lines available, limit set to {}".format(data_file, file_length, test_line_limit))
        print("{:.2f}s taken to count lines\n".format(time.time() - t0))
        return file_length

if __name__ == "__main__":
    main()