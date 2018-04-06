#!/usr/bin/env python
# encoding: utf-8

# This function generates a database given a key_generator, key_count and row_maker
# It is resumable and logs the methods and sessions used to generate a database
# Ideal for building databases which require a long time to make

import functools
import os
import datetime
import signal
import sqlite3
import subprocess
import time
import logging

from collections import OrderedDict

from ihutilities import write_dictionary, git_sha, git_uncommitted_changes
from ihutilities import configure_db, write_to_db, update_to_db, drop_db_tables, read_db

metadata_fields = OrderedDict([
    ("SequenceNumber", "INTEGER PRIMARY KEY"),
    ("key_method", "TEXT"),
    ("make_row_method", "TEXT"),
    ("status", "TEXT"),
    ("duration", "FLOAT"),
    ("line_count", "INTEGER"),
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

user_fields = OrderedDict([
    ("key", "TEXT PRIMARY KEY"),
    ("value", "TEXT")
    ])

logger = logging.getLogger(__name__)

def build_cache(constructors, cache_db, cache_fields, sha, chunk_size=1000, test=True):
    if test:
        output_dir = os.path.dirname(cache_db)
        cache_db = os.path.join(output_dir, "test.sqlite")
        print("** test is True therefore cache_db set to {}".format(cache_db))

    print("Making cache file: {}".format(cache_db), flush=True)
    t0 = time.time()

    # Create database

    db_fields = {"property_data": cache_fields, "metadata":metadata_fields, 
                 "session_log": session_log_fields, "user_fields":user_fields}

    if os.path.isfile(cache_db):
        print("Database file {} already exists, attempting to update. Delete file for a fresh start".format(cache_db))
        configure_db(cache_db, db_fields, tables = list(db_fields.keys()))
    else:
        print("Creating database at {}".format(cache_db), flush=True)
        configure_db(cache_db, db_fields, tables = list(db_fields.keys()))

    # Loop over the constructors
    total_line_count = 0
    for id_, (key_generator, key_count, make_row_method) in enumerate(constructors):
        key_generator_name = get_function_name(key_generator)
        make_row_method_name = get_function_name(make_row_method)

        stage_status = check_stage_status(key_generator, make_row_method, cache_db)
        if stage_status == "Complete":
            print("Flatfile db already updated with {}, continuing to next stage".format(make_row_method_name), flush=True)
            continue
        elif stage_status == "Not started":
            finish_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            metadata = [(id_, key_generator_name, make_row_method_name, "Started", "{:.2f}".format(0), 0, finish_time, 0)]
            print("Trying to add metadata line: {}".format(metadata), flush=True)
            write_to_db(metadata, cache_db, db_fields["metadata"], table="metadata")
            
        t_update0 = time.time()
        print("\nUpdating flatfile db with {}".format(make_row_method_name), flush=True)
        # This is where we make the data
        line_count = updater(id_, key_generator, key_count, make_row_method, cache_db, db_fields, sha, chunk_size)
        # 
        total_line_count += line_count
        # Write final report
        t_update1 = time.time()
        elapsed = t_update1 - t_update0
        finish_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Wrote {0} '{1}' records to {2} in {3:.2f}s".format(line_count, make_row_method_name, os.path.basename(cache_db), elapsed), flush=True)
        metadata = [(id_, key_generator_name, make_row_method_name, "Complete", "{:.2f}".format(elapsed), line_count, finish_time)]
        update_fields = [x for x in db_fields["metadata"].keys()]
        update_to_db(metadata, cache_db, update_fields, table="metadata", key="SequenceNumber")
        # update_to_db(metadata, db_file_path, db_fields["metadata"], table="metadata")


    # Write final report
    t1 = time.time()
    elapsed = t1 - t0
    print("\nWrote a total {0} records to {1} in {2:.2f}s".format(total_line_count, os.path.basename(cache_db), elapsed), flush=True)

    return cache_db

def updater(id_, key_method, get_key_count, make_row_method, cache_db, db_fields, sha, chunk_size):
    key_method_name = get_function_name(key_method)
    make_row_method_name = get_function_name(make_row_method)
    
    # Get a bunch of UPRNs
    key_count = get_key_count()
    # uprn_cursor = get_uprn_cursor(data_source_dictionary[uprn_method])
    print("Found {} keys in {}".format(key_count, key_method_name), flush=True)
    # Set loop control variables
    # chunk_size = 1000 #100000 #1000 #100000 for production, 1000 for test
    line_count = 0
    chunk_count = 0
    test_limit = float('inf') # 10000 # float('inf')
    uprn_types = 1
    line_count_offset = 0
    print("Test_limit set to {}".format(test_limit), flush=True)

    
    # Fetch chunk progress
    chunk_skip = get_chunk_count(id_, cache_db)
    print("Skipping {} chunks".format(chunk_skip), flush=True)
    # ** Skip chunks
    key_chunks = key_method(chunk_size)

    if chunk_skip != 0:
        for i in range(0, chunk_skip):
            print("Skipping chunk {} in ({}, {})".format(i, key_method_name, make_row_method_name), flush=True) 
            key_chunks.__next__()
            line_count_offset = chunk_size * chunk_skip
            chunk_count = chunk_skip

    # Update session log here
    time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session_log_data = [(None, make_row_method_name, time_, time_, sha, chunk_skip, chunk_skip)]
    write_to_db(session_log_data, cache_db, db_fields["session_log"], table="session_log")

    # Pick up session log data
    sql_query = "select * from session_log order by ID desc;"
    results = list(read_db(sql_query, cache_db))
    sessid = results[0]["ID"]
    
    t0 = time.time()

    for keys in key_chunks:
    # for uprns in uprn_source(uprn_method, chunk_size): 
        chunk_count += 1
        # Break for testing
        if line_count > test_limit:
            break

        #if len(uprns) == 0:
        #    break
        data = []
    
        for key in keys:
            if key in ['', None]:
                print("key is blank so continuing",flush=True)
                continue
            
            
            # This is what makes a cache row
            # time.sleep(4/1000)
            data_row = make_row_method(key)
            
            # Location Intelligence returns a list at this point
            # Location Intelligence LIDAR will return a list of lists
            # 
            if len(data_row) == 0:
                continue

            if isinstance(data_row[0], list):
                data.extend(data_row)
                line_count += len(data_row)
            else: 
                data.append(data_row)
                line_count += 1
                
        # Insert record batch
        if len(data) != 0:
            write_to_db(data, cache_db, db_fields["property_data"])
        # Update chunk_count to db metadata
        update_to_db([(chunk_count, id_)], cache_db, ["chunk_count", "SequenceNumber"], table="metadata", key="SequenceNumber")
        # Update current time and chunk count to session log
        time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_to_db([(chunk_count, time_, sessid)], cache_db, ["last_chunk", "end_time", "ID"], table="session_log", key="ID")

        if line_count != 0:
            est_completion_time = ((time.time() - t0) / line_count) * (key_count - (line_count + line_count_offset))
        else:
            est_completion_time = ((time.time() - t0) / 1) * (key_count - (1 + line_count_offset))
            
        total_runtime = ((time.time() - t0) + est_completion_time) / (60 * 60 * 24)
        completion_str = (datetime.datetime.now() + datetime.timedelta(seconds=est_completion_time)).strftime("%Y-%m-%d %H:%M:%S")
        print("{}: {}/{} at {}. Est. completion time: {}. Est. total runtime = {:.2f} days".format(
            make_row_method_name,
            line_count + line_count_offset, 
            key_count,
            datetime.datetime.now().strftime("%H:%M:%S"),
            completion_str,
            total_runtime
            ), flush=True)

        #time.sleep(5)
        
    # close uprn source
    # uprn_cursor.close()
    

    return (line_count + line_count_offset)

def get_chunk_count(id_, cache_db):
    conn = sqlite3.connect(cache_db)
    c = conn.cursor()
    c.execute("select chunk_count from metadata where SequenceNumber = ?;", (id_,))
    result = c.fetchall()
    chunk_count = result[0][0]
    return chunk_count

def check_stage_status(key_method, make_row_method, cache_db):
    key_method_name = get_function_name(key_method)
    make_row_method_name = get_function_name(make_row_method)

    conn = sqlite3.connect(cache_db)
    c = conn.cursor()
    c.execute("select status from metadata where key_method = ? and make_row_method = ?;", (key_method_name, make_row_method_name))
    result = c.fetchall()
    stage_complete = "Not started"
    if len(result) == 1:
        stage_complete = result[0][0]
    c.close()
    return stage_complete

def get_function_name(a_function):
    if isinstance(a_function, functools.partial):
        function_name = a_function.func.__name__
    else:
        function_name = a_function.__name__
    return function_name

if __name__ == "__main__":
    main()
