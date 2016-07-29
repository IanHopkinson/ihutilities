#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import os
import sqlite3
import sys
import time

from collections import OrderedDict
from ihutilities import configure_db, write_to_db, update_to_db, read_db

# This dictionary has field names and field types. It should be reuseable between the configure_db and 
# write_to_db functions

metadata_fields = OrderedDict([
    ("SequenceNumber", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("data_path", "TEXT"),
    ("status", "TEXT"),
    ("start_time", "TEXT"),
    ("finish_time", "TEXT"),
    ("last_write_time", "TEXT")
    ])

def do_etl(db_fields, db_config, data_path, data_field_lookup, mode="production", headers=True, null_equivalents=[""], force=False, separator=",", encoding="utf-8-sig"):
    """This function uploads CSV files to a sqlite or MariaDB/MySQL database

    Args:
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table. 
       data_path (str):
            A file path to the input CSV data
       data_field_lookup (dict):
            A dictionary linking database fields (as the key) to CSV columns (the value),
            if headers exist the values are column names. If headers do not exist then the 
            values are column numbers. If a field ID is specified with value None then it creates
            autoincrement unique key

    Kwargs:
       mode (str): 
            "production" or "test". 
            "test" loads 10000 lines to the database in 1000 line chunks.
            "test" loads 10000 lines to the database in 1000 line chunks. 
       headers (bool): 
            Indicates whether headers are present in the input CSV file
            True indicates headers are present, DictReader is used for import and the data_field_lookup is to field names
            False indicates no headers, csvreader is used for import and the data_field_lookup lookup is to column numbers 
       null_equivalents (list of strings):
            cell contents which should be considered equivalent of null i.e ["-"]
       separator (str):
            a separator for the CSV, default is comma 

    Returns:
       No return value

    Raises:

    Usage:

    """
    print("Starting ETL to database at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), flush=True)
    print("Input file is {}".format(data_path))
    # Scan parameters
    if mode == "production":
        test_line_limit = float('inf') # float('inf')
        chunk_size = 10000 # 10000
        report_size = 10000 # 10000
        print("Measuring length of input file...", flush=True)
        file_length = report_input_length(data_path, test_line_limit)   
    elif mode == "test":
        test_line_limit = 10000 # float('inf')
        chunk_size = 1000 # 10000
        report_size = 1000 # 10000
        print("Test mode so file_length is set to test_line_limit of {}".format(test_line_limit), flush=True)
        file_length = test_line_limit
        # Rename output database if we are in test mode
        if isinstance(db_config, str):
            db_config = db_config.replace(".sqlite", "-test.sqlite")
            print("Renamed output database to {} because we are in test mode".format(db_config), flush=True)
        else:
            db_config["db_name"] = db_config["db_name"] + "_test"
            print("Renamed output database to {} because we are in test mode".format(db_config["db_name"]), flush=True)
    else:
        print("Mode should be either 'test' or 'production', mode supplied was '{}'".format(mode))
        sys.exit()

    # We're implicitly writing data to "property_data" because we didn't provide a tables argument
    revised_db_fields = {}
    revised_db_fields["property_data"] = db_fields
    revised_db_fields["metadata"] = metadata_fields   

    configure_db(db_config, revised_db_fields, tables = ["property_data", "metadata"], force=force)

    # Get on with the main business
    t0 = time.time()
    t_last = t0
    data = []
    line_count = 0
    lines_dropped = 0

    primary_key_set = set()
    duplicate_primary_keys = set()
    primary_key = get_primary_key_from_db_fields(db_fields)

    if primary_key == "ID" and data_field_lookup["ID"] is None:
        autoinc = True
    else:
        autoinc = False

    # Write start to metadata table
    id_ = None
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metadata = [(id_, data_path, "Started", start_time, "", "")]
    write_to_db(metadata, db_config, revised_db_fields["metadata"], table="metadata")

    with open(data_path, encoding=encoding) as f:
        if headers:
            rows = csv.DictReader(f, delimiter=separator)
        else:
            rows = csv.reader(f, delimiter=separator)
        # Loop over input rows
        try:
            for i, row in enumerate(rows):
                # print("Read row {}".format(i), flush=True)
                new_row = OrderedDict([(x,None) for x in db_fields.keys()]) 
                # zip input row into output row
                for output_key in new_row.keys():
                    # This inserts blank fields
                    if data_field_lookup[output_key] is not None:
                        if not isinstance(data_field_lookup[output_key], list):
                            value = row[data_field_lookup[output_key]]
                            if value in null_equivalents:
                                value = None
                        # If output_key corresponds to a POINT field we need to process a two element array
                        if db_fields[output_key] == "POINT":
                            new_row[output_key] = make_point(row, data_field_lookup[output_key])
                        # If output_key corresponds to an INTEGER then remove any commas in input
                        elif db_fields[output_key].lower() == "integer" and value is not None:
                            new_row[output_key] = int(value.replace(",", ""))
                        else:
                            if row[data_field_lookup[output_key]] != "":
                                new_row[output_key] = value
                    # If we have a field called ID as Primary Key and there is no lookup
                    # for it we assume it is a synthetic key and put in an autoincrement value
                    if autoinc:
                        new_row[primary_key] = None
                # Decide whether or not to write new_row
                if autoinc or (new_row[primary_key] not in primary_key_set):
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
                    write_to_db(data, db_config, db_fields)
                    data = []

                # Break if we have reached test_line_limit
                if i > test_line_limit:
                    break
        except Exception as ex:
            print("Encountered exception '{}' at line_count = {}".format(ex, line_count), flush=True)
            #print("Row: {}".format(row))
            #for key in row.keys():
            #    print("Key: '{:30}', value: '{:}'".format(key, row[key]))
            # raise
            print("Carrying on regardless", flush=True)
            raise   

    # Final write to database
    write_to_db(data, db_config, db_fields)

    # Write a final report
    t1 = time.time()
    elapsed = t1 - t0
    print("\nWrote a total {} lines to the database in {:.2f}s".format(line_count, elapsed), flush=True)
    print("Dropped {} lines because they contained duplicate primary key ({})".format(lines_dropped, primary_key), flush=True)

    finish_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Now we are autoincrementing the SequenceNumber field, we need to do a read_db to find the value
    sql_query = "select max(SequenceNumber) as actual_id from metadata;"
    actual_id = list(read_db(sql_query, db_config))[0]["actual_id"]

    metadata = [(actual_id, data_path, "Complete", start_time, finish_time, finish_time)]
    update_fields = [x for x in revised_db_fields["metadata"].keys()]
    update_to_db(metadata, db_config, update_fields, table="metadata", key="SequenceNumber")

def make_point(row, data_field_lookup):
    try:
        easting = float(row[data_field_lookup[0]])
    except ValueError:
        easting = 0
    try:
        northing = float(row[data_field_lookup[1]])
    except ValueError:
        northing = 0
    point = "POINT({} {})".format(easting, northing)
    return point

def get_primary_key_from_db_fields(db_fields):
    primary_key = None
    for key, value in db_fields.items():
        if "PRIMARY KEY" in value.upper():
            primary_key = key

    if primary_key is None:
        raise RuntimeError("No primary key found")

    return primary_key

def report_input_length(data_path, test_line_limit):
    t0 = time.time()
    with open(data_path, encoding='utf-8-sig') as f:
        rows = csv.reader(f)
        file_length = sum(1 for row in rows) - 1 #Take off the header line
        data_file = os.path.basename(data_path)
        print("Importing '{}'. {} lines available, limit set to {}".format(data_file, file_length, test_line_limit), flush=True)
        print("{:.2f}s taken to count lines\n".format(time.time() - t0), flush=True)
        return file_length

if __name__ == "__main__":
    main()