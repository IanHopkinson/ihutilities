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
from ihutilities import configure_db, write_to_db, update_to_db, drop_db_tables, read_db, delete_from_db

from dask.distributed import Client, as_completed

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

chunk_log_fields = OrderedDict([
    ("chunk_number", "INTEGER"),
    ("chunk_udprn", "INTEGER"),
    ("chunk_umrrn", "INTEGER")
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
                 "session_log": session_log_fields, "user_fields":user_fields,
                 "chunk_log": chunk_log_fields}

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
    # chunk_skip = get_chunk_count(id_, cache_db)
    # print("Skipping {} chunks".format(chunk_skip), flush=True)
    # ** Skip chunks
    key_chunks = list(key_method(chunk_size))

    # Count how much data is in the table
    sql_query = "select count(*) as cnt from property_data"
    tmp = list(read_db(sql_query, cache_db))
    done_count = tmp[0]["cnt"]

    # If there is no data in the property_data table then we need to build the chunk_log
    if done_count == 0:
        # Generate a list of chunk_ids and push to the chunk_log table
        chunk_log = []
        for i, keys in enumerate(key_chunks):
            chunk_id_udprn = keys[0][0]
            chunk_id_umrrn = keys[0][1]
            chunk_log_row = (i, chunk_id_udprn, chunk_id_umrrn)
            chunk_log.append(chunk_log_row)
            if (i != 0) & (i % 1000) == 0:
                write_to_db(chunk_log, cache_db, db_fields["chunk_log"], table="chunk_log")        
                chunk_log = []

        if len(chunk_log) != 0:
            write_to_db(chunk_log, cache_db, db_fields["chunk_log"], table="chunk_log")
        n_key_chunks = i + 1
        ### End chunk_log table generation
        todo_chunk_list = [x for x in range(0, i + 1)]
    else:
        n_key_chunks = len(key_chunks)
        sql_query = "select chunk_number from chunk_log"
        tmp = read_db(sql_query, cache_db)
        todo_chunk_list = [x["chunk_number"] for x in tmp]

    n_chunk_todo = len(todo_chunk_list)
    chunk_skip = n_key_chunks - n_chunk_todo
    line_count_offset = chunk_skip * chunk_size
    print("{} chunks will be skipped because they've been done already".format(chunk_skip), flush=True)

    # Update session log here
    time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session_log_data = [(None, make_row_method_name, time_, time_, sha, chunk_skip, chunk_skip)]
    write_to_db(session_log_data, cache_db, db_fields["session_log"], table="session_log")

    # Pick up session log data
    sql_query = "select * from session_log order by ID desc;"
    results = list(read_db(sql_query, cache_db))
    sessid = results[0]["ID"]
    
    t0 = time.time()

    # Launch chunks
    client = Client(processes=False)
    futures = []
    chunk_lookup = {}
    
    for i, keys in enumerate(key_chunks):
    # for uprns in uprn_source(uprn_method, chunk_size): 
        # Filter out chunks that have already been done
        if i not in todo_chunk_list:
            print("Skipping chunk {}".format(i), flush=True)
            continue
        chunk_count += 1
        # Break for testing
        if line_count > test_limit:
            break

        #if len(uprns) == 0:
        #    break
        t1 = time.time()
        elapsed = t1 - t0
        chunk_id_udprn = keys[0][0]
        chunk_id_umrrn = keys[0][1]
        chunk_lookup[(chunk_id_udprn, chunk_id_umrrn)] = i
        logger.info("Dispatching chunk {0}, commencing ({1}, {2}) at {3:.1f} seconds".format(i, chunk_id_udprn, chunk_id_umrrn, elapsed))
        # subset = (UDPRNs[i:i + chunk_size]) 
        futures.append(client.submit(make_modelled_rows_block, make_row_method, keys))
        # Wait for the futures to come in every so often
        #if (i != 0) and (i % epoch_size) == 0:
        #    logging.info("Waiting for an epoch of {} chunks".format(epoch_size))
        # chunk_lookup[subset[0]] = i
    # Collect chunks
    # Receive and measure modelled rows
    t0 = time.time()
    n_chunks_received = 0
    n_chunks = len(futures)
    for future in as_completed(futures):    
        n_chunks_received += 1
        data = future.result()
        line_count += len(data)
        chunk_id_udprn = data[0][1]
        chunk_id_umrrn = data[0][2]
        chunk_loc = chunk_lookup[(chunk_id_udprn, chunk_id_umrrn)]
        elapsed = time.time() - t0
        # logger.info("Received chunk {} of {} commencing {} at {:.1f} seconds".format(n_chunks_received, n_chunks, chunk_loc, elapsed))
        # Insert record batch
        if len(data) != 0:
            write_to_db(data, cache_db, db_fields["property_data"])
        # Update chunk_count to db metadata
        update_to_db([(chunk_count, id_)], cache_db, ["chunk_count", "SequenceNumber"], table="metadata", key="SequenceNumber")
        # Update current time and chunk count to session log
        time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_to_db([(n_chunks_received + chunk_skip, time_, sessid)], cache_db, ["last_chunk", "end_time", "ID"], table="session_log", key="ID")
        # Delete row from chunk_log 
        sql_query = "delete from chunk_log where chunk_number={}".format(chunk_loc)
        delete_from_db(sql_query, cache_db)

        if line_count != 0:
            est_completion_time = ((time.time() - t0) / line_count) * (key_count - (line_count + line_count_offset))
        else:
            est_completion_time = ((time.time() - t0) / 1) * (key_count - (1 + line_count_offset))
            
        total_runtime = ((time.time() - t0) + est_completion_time) / (60 * 60 * 24)
        completion_str = (datetime.datetime.now() + datetime.timedelta(seconds=est_completion_time)).strftime("%Y-%m-%d %H:%M:%S")
        print("{}: {}/{} at {}. Est. completion time: {}. Est. total runtime = {:.2f} days".format(
            make_row_method_name,
            n_chunks_received, 
            n_chunk_todo,
            datetime.datetime.now().strftime("%H:%M:%S"),
            completion_str,
            total_runtime
            ), flush=True)
    
    return (line_count + line_count_offset)

def make_modelled_rows_block(make_row_method, keys):
    data_rows = []
    line_count = 0
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
            data_rows.extend(data_row)
            line_count += len(data_row)
        else: 
            data_rows.append(data_row)
            line_count += 1

    return data_rows

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
