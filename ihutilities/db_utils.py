#!/usr/bin/env python
# encoding: utf-8

import os
import sqlite3
import mysql.connector
from mysql.connector import errorcode

from collections import OrderedDict

db_config_template = {"db_name": "test",
             "db_user": "root",
             "db_pw_environ": "MARIA_DB_PASSWORD",
             "db_host": "127.0.0.1",
             "db_conn": None,
             "db_type": "mysql",
             "db_path": None
            }

def configure_db(db_config, db_fields, tables="property_data", force=False):
    """This function sets up a sqlite or MariaDB/MySQL database

    Args:
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Kwargs:
       tables (string or list of strings): 
            names of tables required, keys to db_fields
       force (bool): 
            If using sqlite, force=True deletes existing files of db_config 

    Returns:
       db_config structure

    Raises:
       AttributeError, KeyError

    A really great idea.  A way you might use me is

    >>> print public_fn_with_googley_docstring(name='foo', state=None)
    0

    BTW, this always returns 0.  **NEVER** use with :class:`MyPublicClass`.

    """
    # Cunning polymorphism: 
    # If we get a list and string then we convert them to a dictionary and a list
    # for backward compatibility

    if isinstance(tables, str):
        tables = [tables]
        db_fields = {tables[0]: db_fields}
    # Convert old db_path string to db_config dictionary
    db_config = _normalise_config(db_config)

    # Delete database if force is true
    if db_config["db_type"] == "sqlite":
        if os.path.isfile(db_config["db_path"]) and force:
            os.remove(db_config["db_path"])
        conn = _make_connection(db_config)
    # Default behaviour for mysql/mariadb is not to drop database
    elif db_config["db_type"] == "mysql" or db_config["db_type"] == "mariadb":
        conn = _make_connection(db_config)
        cursor = conn.cursor()
        #cursor.execute("DROP DATABASE IF EXISTS {}".format(db_config["db_name"]))
        #conn.commit()
            # This creates the database if it doesn't exist
        try:
            conn.database = db_config["db_name"]   
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                try:
                    cursor.execute(
                        "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_config["db_name"]))
                except mysql.connector.Error as err:
                    print("Failed creating database: {}".format(err))
                    exit(1)
                conn.database = db_config["db_name"]
            else:
                print(err)
                exit(1)

    # Create tables, as specified
    _create_tables_db(db_config, db_fields, tables, force)
    # Close connection? or return db_config
    
    db_config["db_conn"].commit()
    #db_config["db_conn"].close()

    return db_config

def _normalise_config(db_config):
    if isinstance(db_config, str):
        db_path = db_config
        db_config = db_config_template.copy()
        db_config["db_type"] = "sqlite"
        db_config["db_path"] = db_path
    return db_config

def _make_connection(db_config):

    if db_config["db_type"] == "sqlite":
        db_config["db_conn"] = sqlite3.connect(db_config["db_path"])
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":

        if db_config["db_conn"] is None:
            password = os.environ[db_config["db_pw_environ"]]
            conn = mysql.connector.connect( user=db_config["db_user"], 
                                        password=password,
                                        host=db_config["db_host"])

            db_config["db_conn"] = conn
        else:
            conn = db_config["db_conn"]

        # Bit messy, sometimes we make a connection without db existing
        try:
            conn.database = db_config["db_name"]
        except mysql.connector.Error as err:
            if err.errno != errorcode.ER_BAD_DB_ERROR:
                raise


    return db_config["db_conn"]

def _create_tables_db(db_config, db_fields, tables, force):
    if db_config["db_type"] == "sqlite":
        table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';"
        DB_CREATE_TAIL = ")"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        table_check_query = "SELECT table_name as name FROM information_schema.tables WHERE table_name = '{}';"
        DB_CREATE_TAIL = ") ENGINE = MyISAM"

    conn = db_config["db_conn"]
    cursor = conn.cursor()
    for table in tables:
        DB_CREATE_ROOT = "CREATE TABLE {} (".format(table)
        
        DB_CREATE = DB_CREATE_ROOT
        for k,v in db_fields[table].items():
            DB_CREATE = DB_CREATE + " ".join([k,v]) + ","

        DB_CREATE = DB_CREATE[0:-1] + DB_CREATE_TAIL
        if force:
            cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
        
        cursor.execute(table_check_query.format(table))
        result = cursor.fetchall()
        if len(result) != 0 and result[0][0] == table:
            table_exists = True
        else:
            table_exists = False

        if not table_exists:    
            cursor.execute(DB_CREATE)

    db_config["db_conn"].commit()

def write_to_db(data, db_config, db_fields, table="property_data"):
    """
    we write data into the property_data table from an array of tuples of
    value entries 
    """
    db_config = _normalise_config(db_config)
    if db_config["db_type"] == "sqlite":
        ONE_PLACEHOLDER = "?,"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        ONE_PLACEHOLDER = "%s,"
   
    conn = _make_connection(db_config)
    cursor = conn.cursor()

    DB_INSERT_ROOT = "INSERT INTO {} (".format(table)
    DB_INSERT_MIDDLE = ") VALUES ("
    DB_INSERT_TAIL = ")"

    DB_FIELDS = DB_INSERT_ROOT
    DB_PLACEHOLDERS = DB_INSERT_MIDDLE

    for k in db_fields.keys():
        DB_FIELDS = DB_FIELDS + k + ","
        if db_fields[k] in ["POINT", "POLYGON", "LINESTRING"]:
            DB_PLACEHOLDERS = DB_PLACEHOLDERS + "(GeomFromText(%s)),"
        else:
            DB_PLACEHOLDERS = DB_PLACEHOLDERS + ONE_PLACEHOLDER

    INSERT_statement = DB_FIELDS[0:-1] + DB_PLACEHOLDERS[0:-1] + DB_INSERT_TAIL

    cursor.executemany(INSERT_statement, data)

    conn.commit()
    # conn.close()

def update_to_db(data, db_config, db_fields, table="property_data", key="UPRN"):
    """
    We *update* data into the property_data table from an array of dictionaries of
    field: value entries

    db_fields is a list of field names, the last in the list is the join keys
    data is a list of lists of the elements to be inserted 
    """
    db_config = _normalise_config(db_config)
    if db_config["db_type"] == "sqlite":
        DB_UPDATE_TAIL = " WHERE {} = ?".format(key)
        PLACEHOLDER = " = ?,"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        DB_UPDATE_TAIL = " WHERE {} = %s".format(key)
        PLACEHOLDER = " = %s,"

    conn = _make_connection(db_config)
    cursor = conn.cursor()
    DB_UPDATE_ROOT = "UPDATE {} SET ".format(table)
    
    key_index = db_fields.index(key)

    
    for row in data:
        # print(update_statement, [x for x in row])
        key_val = row[key_index]
        update_fields = []
        update_data = []
        for i, _ in enumerate(row):
            if i != key_index and row[i] is not None:
                update_fields.append(db_fields[i])
                update_data.append(row[i])
                
        DB_FIELDS = DB_UPDATE_ROOT
        for k in update_fields:
            DB_FIELDS = DB_FIELDS + k + PLACEHOLDER 
            update_statement = DB_FIELDS[0:-1] + DB_UPDATE_TAIL

        update_data.append(key_val)
        if len(update_fields) != 0:
            cursor.execute(update_statement, update_data)

    conn.commit()
    #conn.close()

def drop_db_tables(file_path, tables):
    conn = sqlite3.connect(file_path)
    for table in tables:
        conn.execute('DROP TABLE IF EXISTS {}'.format(table))
    conn.close()

def finalise_db(db_config, index_name="idx_postcode", table="property_data", colname="postcode" ):
    db_config = _normalise_config(db_config)

    conn = _make_connection(db_config)
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX {index_name} on {table}({colname})'
        .format(index_name=index_name, table=table, colname=colname))
    conn.commit()
    #conn.close()

def read_db(sql_query, db_config):
    db_config = _normalise_config(db_config)
    conn = _make_connection(db_config)
    cursor = conn.cursor()

    cursor.execute(sql_query)

    colnames = [x[0] for x in cursor.description]

    while True:
        row = cursor.fetchone()
        if row is not None:
            labelled_row = OrderedDict(zip(colnames, row))
            yield labelled_row
        else:
            raise StopIteration
