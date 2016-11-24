#!/usr/bin/env python
# encoding: utf-8

import csv
import datetime
import io
import logging
import os
import sqlite3
import sys
import time
import zipfile

from collections import OrderedDict
from ihutilities import (configure_db, write_to_db, update_to_db, read_db, 
                        calculate_file_sha, _normalise_config, check_mysql_database_exists,
                        get_a_file_handle, split_zipfile_path)

# This dictionary has field names and field types. It should be reuseable between the configure_db and 
# write_to_db functions

metadata_fields = OrderedDict([
    ("SequenceNumber", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("data_path", "TEXT"),
    ("datafile_sha", "TEXT"),
    ("status", "TEXT"),
    ("start_time", "TEXT"),
    ("finish_time", "TEXT"),
    ("last_write_time", "TEXT"),
    ("chunk_count", "INTEGER")
    ])

session_log_fields = OrderedDict([
    ("ID", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("make_row_method", "TEXT"),
    ("start_time", "TEXT"),
    ("end_time", "TEXT"),
    ("sha", "TEXT"),
    ("first_chunk", "TEXT"),
    ("last_chunk", "FLOAT"),
    ])

logger = logging.getLogger(__name__)

def make_row(input_row, data_path, data_field_lookup, db_fields, null_equivalents, autoinc, primary_key):
    new_row = OrderedDict([(x,None) for x in db_fields.keys()])
     # zip input row into output row
    for output_key in new_row.keys():
        # This inserts blank fields
        if data_field_lookup[output_key] is not None:
            if not isinstance(data_field_lookup[output_key], list):
                try:
                    value = input_row[data_field_lookup[output_key]]
                except KeyError:
                    logger.critical("Required data field '{}' not found in input data = {}".format(data_field_lookup[output_key], input_row))
                    raise
                if value in null_equivalents:
                    value = None
            # If output_key corresponds to a POINT field we need to process a two element array
            if db_fields[output_key] == "POINT":
                new_row[output_key] = make_point(input_row, data_field_lookup[output_key])
            # If output_key corresponds to an INTEGER then remove any commas in input
            elif db_fields[output_key].lower() == "integer" and value is not None:
                new_row[output_key] = int(value.replace(",", ""))
            else:
                if input_row[data_field_lookup[output_key]] != "":
                    new_row[output_key] = value
        # If we have a field called ID as Primary Key and there is no lookup
        # for it we assume it is a synthetic key and put in an autoincrement value
        if autoinc:
            new_row[primary_key] = None

    return new_row

def get_source_generator(data_path, headers, separator, encoding):
    # See data manager for detecting zip files, and then picking up the right part
    #
    fh = get_a_file_handle(data_path, encoding=encoding)
    with fh:
        if headers:
            rows = csv.DictReader(fh, delimiter=separator)
        else:
            rows = csv.reader(fh, delimiter=separator)

        for row in rows:
            yield row

def do_etl(db_fields, db_config, data_path, data_field_lookup, 
            mode="production", headers=True, null_equivalents=[""], force=False, 
            separator=",", encoding="utf-8-sig", table=None,
            rowmaker=make_row, rowsource=get_source_generator):
    """This function uploads CSV files to a sqlite or MariaDB/MySQL database

    Args:
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table. 
       data_path (str):
            A file path to the input CSV data or a zip file containing a CSV file with the same name
       data_field_lookup (dict):
            A dictionary linking database fields (as the key) to CSV columns (the value),
            if headers exist the values are column names. If headers do not exist then the 
            values are column numbers. If a field ID is specified with value None then it creates
            autoincrement unique key

    Kwargs:
       mode (str): 
            "production" or "test". 
            "test" loads 10000 lines to the database in 1000 line chunks.
       headers (bool): 
            Indicates whether headers are present in the input CSV file
            True indicates headers are present, DictReader is used for import and the data_field_lookup is to field names
            False indicates no headers, csvreader is used for import and the data_field_lookup lookup is to column numbers 
       null_equivalents (list of strings):
            cell contents which should be considered equivalent of null i.e ["-"]
       separator (str):
            a separator for the CSV, default is comma
       force (bool):
            if True then database dropped before ETL, if False then no op if data file has already been uploaded, data appended
            if the file has not yet been uploaded.
       encoding (str):
            the character encoding for the input file
       rowmaker (function):
            the rowmaker function takes an input data row and converts it to a line for the output database. The function call is:
            rowmaker(row, data_path, data_field_lookup, db_fields, null_equivalents, autoinc, primary_key)
       rowsource (function):
            the rowsource function yields input data rows which are handed off to the rowmaker to make database rows. The function call is:
            get_source_generator(data_path, headers, separator, encoding)

    Returns:
       db_config (dict):
            a db_config structure with, if in test mode this will contain the modified database name/path
       status (string):
            "Completed" if the ETL runs to completion, "Already done" if ETL has already been done

    Raises:

    Usage:

    """
    logger.info("Starting ETL to database at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    logger.info("Input file is {}".format(data_path))
    # Scan parameters
    if mode == "production":
        test_line_limit = float('inf') # float('inf')
        chunk_size = 10000 # 10000
        report_size = 10000 # 10000
        logger.info("Measuring length of input file...")
        file_length = report_input_length(rowsource, test_line_limit, data_path, headers, separator, encoding)   
    elif mode == "test":
        test_line_limit = 10000 # float('inf')
        chunk_size = 1000 # 10000
        report_size = 1000 # 10000
        logger.info("Test mode so file_length is set to test_line_limit of {}".format(test_line_limit))
        file_length = test_line_limit
        # Rename output database if we are in test mode but not if it already ends with test
        if isinstance(db_config, str) and not db_config.endswith("-test.sqlite"):
            db_config = db_config.replace(".sqlite", "-test.sqlite")
            logger.info("Renamed output database to {} because we are in test mode".format(db_config))
        elif not db_config["db_name"].endswith("_test"):
            db_config["db_name"] = db_config["db_name"] + "_test"
            logger.info("Renamed output database to {} because we are in test mode".format(db_config["db_name"]))
    else:
        logger.critical("Mode should be either 'test' or 'production', mode supplied was '{}'".format(mode))
        sys.exit()

    db_config = _normalise_config(db_config)
    
    # If force is false then return if ETL on this file has already been done
    zip_path, name_in_zip = split_zipfile_path(data_path)

    datafile_sha = calculate_file_sha(data_path)
    already_done = check_if_already_done(data_path, db_config, datafile_sha)
    
    if already_done and not force:
        logger.warning("Data file has already been uploaded to database, therefore returning. Delete database to allow ETL")
        return db_config, "Already done"

    # If the table argument is None we assume we are writing to the property_data table and that db_fields describes one flat level table
    if table is None:
        table = "property_data"
        revised_db_fields = {}
        revised_db_fields["property_data"] = db_fields
        revised_db_fields["metadata"] = metadata_fields
        revised_db_fields["session_log"] = session_log_fields   
        tables = ["property_data", "metadata", "session_log"]
    else:
        revised_db_fields = db_fields.copy()
        revised_db_fields["metadata"] = metadata_fields   
        revised_db_fields["session_log"] = session_log_fields
        tables = list(db_fields.keys())
        tables.append("metadata")
        tables.append("session_log")

    configure_db(db_config, revised_db_fields, tables=tables, force=force)

    # Get on with the main business
    t0 = time.time()
    t_last = t0
    data = []
    line_count = 0
    lines_dropped = 0

    primary_key_set = set()
    duplicate_primary_keys = set()
    primary_key = get_primary_key_from_db_fields(revised_db_fields[table])

    if primary_key == "ID" and data_field_lookup["ID"] is None:
        autoinc = True
    else:
        autoinc = False

    # Get metadata id_ back out of the database
    sql_query = "select * from metadata order by SequenceNumber desc;"
    results = list(read_db(sql_query, db_config))
    if len(results) == 0:
    # Write start to metadata table
        id_ = None
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata = [(id_, data_path, datafile_sha,"Started", start_time, "", "", 0)]
        write_to_db(metadata, db_config, revised_db_fields["metadata"], table="metadata")
        id_ = 1
    else:
        id_ = results[0]["SequenceNumber"]
        start_time = results[0]["SequenceNumber"]



    # ** Add in session log code
    # Fetch chunk progress
    chunk_skip = get_chunk_count(id_, db_config)
    chunk_count = chunk_skip
    if chunk_skip != 0:
        line_count_offset = (chunk_size * chunk_skip)
        line_count = line_count_offset
    else:
        line_count_offset = 0
        line_count = 0

    logging.info("Skipping {} chunks ({} lines)".format(chunk_skip, line_count))
    # # ** Skip chunks
    # key_chunks = key_method(chunk_size)

    # Update session log here
    time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session_log_data = [(None, rowsource.__name__, time_, time_, datafile_sha, chunk_skip, chunk_skip)]
    write_to_db(session_log_data, db_config, revised_db_fields["session_log"], table="session_log")

    # Pick up session log data
    sql_query = "select * from session_log order by ID desc;"
    results = list(read_db(sql_query, db_config))
    sessid = results[0]["ID"]

    #**End session log code

    rows = rowsource(data_path, headers, separator, encoding)
        # Loop over input rows
    try:
        for i, row in enumerate(rows):
            # Line skipping code goes here
            if i < line_count_offset:
                continue

            
            # Zip the input data into a row for the database
            new_row =  rowmaker(row, data_path, data_field_lookup, revised_db_fields[table], null_equivalents, autoinc, primary_key)
           
            # Decide whether or not to write new_row
            if autoinc or (new_row[primary_key] not in primary_key_set):
                line_count += 1
                data.append(([x for x in new_row.values()]))
                primary_key_set.add(new_row[primary_key])
            else:
                #print("UPRN is a duplicate: {}".format(new_row["UPRN"]))
                duplicate_primary_keys.add(new_row[primary_key])
                lines_dropped += 1
                logger.warning("Lines dropped = {}, do not use resume".format(lines_dropped))
                #print("UPRN = {} has already been seen".format(row[0])) 

            # Write an interim report
            if (line_count % report_size) ==0:
                est_completion_time = ((time.time() - t0) / line_count) * (min(file_length, test_line_limit) - line_count)
                completion_str = (datetime.datetime.now() + datetime.timedelta(seconds=est_completion_time)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Wrote {}/{} at ({}). Estimated completion time: {}".format(
                        line_count, 
                        file_length,
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        completion_str))
                t_last = time.time()

            # Write a chunk to the database            
            if (line_count % chunk_size) == 0:
                write_to_db(data, db_config, revised_db_fields[table], table=table)
                chunk_count += 1
                # Update chunk_count to db metadata
                update_to_db([(chunk_count, id_)], db_config, ["chunk_count", "SequenceNumber"], table="metadata", key="SequenceNumber")
                # Update current time and chunk count to session log
                time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_to_db([(chunk_count, time_, sessid)], db_config, ["last_chunk", "end_time", "ID"], table="session_log", key="ID")
                data = []

            # Break if we have reached test_line_limit
            if i > test_line_limit:
                break
    except Exception as ex:
        logger.critical("Encountered exception '{}' at line_count = {}".format(ex, line_count))
        #print("Row: {}".format(row))
        #for key in row.keys():
        #    print("Key: '{:30}', value: '{:}'".format(key, row[key]))
        # raise
        # print("Carrying on regardless", flush=True)
        raise   

    # Final write to database
    write_to_db(data, db_config, revised_db_fields[table], whatever=True, table=table)

    # Write a final report
    t1 = time.time()
    elapsed = t1 - t0
    logger.info("Wrote a total {} lines to the database in {:.2f}s".format(line_count, elapsed))
    if lines_dropped > 0:
        logger.warning("Dropped {} lines because they contained duplicate primary key ({})".format(lines_dropped, primary_key))

    finish_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Now we are autoincrementing the SequenceNumber field, we need to do a read_db to find the value
    sql_query = "select max(SequenceNumber) as actual_id from metadata;"
    actual_id = list(read_db(sql_query, db_config))[0]["actual_id"]

    metadata = [(actual_id, data_path, datafile_sha,"Complete", start_time, finish_time, finish_time)]
    update_fields = [x for x in revised_db_fields["metadata"].keys()]
    update_to_db(metadata, db_config, update_fields, table="metadata", key="SequenceNumber")

    return db_config, "Completed"

def check_if_already_done(data_path, db_config, datafile_sha):
    db_config = _normalise_config(db_config)
    status = False
    # Check for the existance of the database, return False if they don't exist
    if db_config["db_type"] == "sqlite":
        if not os.path.isfile(db_config["db_path"]):
            return False
    elif db_config["db_type"] == "mysql" or db_config["db_type"] == "mariadb":
        if not check_mysql_database_exists(db_config):
            return False

    # Look for the datafile_sha in the metadata table and if it exists, return True
    sql_query = "select * from metadata where datafile_sha = '{}' and status = 'Complete'".format(datafile_sha)
    results = list(read_db(sql_query, db_config))

    if len(results) == 1:
        return True
    else:
        return False

    return status

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

def report_input_length(rowsource, test_line_limit, data_path, headers, separator, encoding):
    t0 = time.time()

    file_length = sum(1 for row in rowsource(data_path, headers, separator, encoding)) - 1 #Take off the header line
    logger.info("{} lines available, limit set to {}".format(file_length, test_line_limit))
    logger.info("{:.2f}s taken to count lines\n".format(time.time() - t0))
    return file_length

def make_dbfields(file_path):
    fh = get_a_file_handle(file_path)

    # Sniff headers
    with fh:
        reader = csv.reader(fh)
        headers = next(reader, None)

    DB_FIELDS_ARRAY = []
    data_field_lookup_array = []

    for source_field in headers:
        destination_field = source_field.replace("-", "_").replace(" ", "_").replace("(", "_").replace(")", "_").replace(".", "_").replace("/", "_").replace(",", "_")
        DB_FIELDS_ARRAY.append((destination_field, "TEXT"))
        data_field_lookup_array.append((destination_field, source_field))

    DB_FIELDS = OrderedDict(DB_FIELDS_ARRAY)
    data_field_lookup = OrderedDict(data_field_lookup_array)

    return DB_FIELDS, data_field_lookup

def get_chunk_count(id_, cache_db):
    sql_query = "select chunk_count from metadata where SequenceNumber = {};".format(id_) 
    result = list(read_db(sql_query, cache_db))
    
    chunk_count = result[0]["chunk_count"]
    return chunk_count
