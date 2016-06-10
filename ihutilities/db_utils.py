#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to databases
"""
import os
import time
import sqlite3
import logging
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
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Kwargs:
       tables (string or list of strings): 
            names of tables required, keys to db_fields
       force (bool): 
            If using sqlite, force=True deletes existing files of db_config 

    Returns:
       db_config structure, in particular with the db_conn field populated for MariaDB/MySQL

    Raises:

    Usage:
        >>> db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),                   
        ])
        >>> db_filename = "test_config_db.sqlite"
        >>> db_dir = "ihutilities\\fixtures"
        >>> db_file_path = os.path.join(db_dir, db_filename)
        >>> configure_db(db_file_path, self.db_fields, tables="test")
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
        # Make connection now wraps in creation of a new database if it doesn't exist
        conn = _make_connection(db_config)
        cursor = conn.cursor()

    # Create tables, as specified
    _create_tables_db(db_config, db_fields, tables, force)
    # Close connection? or return db_config
    
    db_config["db_conn"].commit()
    #db_config["db_conn"].close()

    return db_config

def write_to_db(data, db_config, db_fields, table="property_data"):
    """
    This function writes a list of rows to a sqlite or MariaDB/MySQL database

    Args:
       data (list of lists):
            List of lists to write to database.
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Kwargs:
       table (str): 
            name of table to which we are writing, key to db_fields

    Returns:
       No return value

    Raises:

    Comments:
        If data is prepared as a dictionary then it can be converted using:
        >>> ([x for x in new_row.values()])
    Usage:
        >>> db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),                   
        ])
        >>> db_filename = "test_write_db.sqlite"
        >>> db_dir = "ihutilities\\fixtures"
        >>> db_file_path = os.path.join(db_dir, db_filename)
        >>> data = [(1, 2, "hello"),
                    (2, 3, "Fred"),
                    (3, 3, "Beans")]
        >>> write_to_db(data, db_file_path, db_fields, table="test")
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
        if db_fields[k] in ["POINT", "POLYGON", "LINESTRING", "MULTIPOLYGON"]:
            DB_PLACEHOLDERS = DB_PLACEHOLDERS + "(GeomFromText(%s)),"
        else:
            DB_PLACEHOLDERS = DB_PLACEHOLDERS + ONE_PLACEHOLDER

    INSERT_statement = DB_FIELDS[0:-1] + DB_PLACEHOLDERS[0:-1] + DB_INSERT_TAIL

    cursor.executemany(INSERT_statement, data)

    conn.commit()
    # conn.close()

def update_to_db(data, db_config, db_fields, table="property_data", key="UPRN"):
    """
    This function updates rows in a sqlite or MariaDB/MySQL database

    Args:
       data (list of lists):
            List of lists of data to update to database, order matches db_fields
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict):
            A list of fieldnames in an OrderedDict containing the fields to update and
            the key field in the order in which the fields are presented in the data lists

    Kwargs:
       table (str): 
            name of table to which we are writing, key to db_fields
       key (str):
            the field which forms the key of the update

    Returns:
       No return value

    Raises:

    Usage:
        >>> db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),                   
        ])
        >>> db_filename = "test_write_db.sqlite"
        >>> db_dir = "ihutilities\\fixtures"
        >>> db_file_path = os.path.join(db_dir, db_filename)
        >>> data = [(1, 2, "hello"),
                    (2, 3, "Fred"),
                    (3, 3, "Beans")]
        >>> update_fields = ["Addr1", "UPRN"]
        >>> update = [("Some", 3)] 
        >>> update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")
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

def finalise_db(db_config, index_name="idx_postcode", table="property_data", colname="postcode", spatial=False):
    """
    This function creates an index in a sqlite or MariaDB/MySQL database

    Args:
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template

    Kwargs:
       index_name (str): 
            name of the index to be created
       table (str):
            the table on which the index is to be created
       colname (str):
            the column on which the index is to be created
       spatial (bool):
            True for a spatial index, false otherwise

    Returns:
       No return value

    Raises:

    Usage:
    """

    db_config = _normalise_config(db_config)

    conn = _make_connection(db_config)
    cursor = conn.cursor()
    if spatial:
        cursor.execute('CREATE SPATIAL INDEX {index_name} on {table}({colname})'
            .format(index_name=index_name, table=table, colname=colname))
    else:
        cursor.execute('CREATE INDEX {index_name} on {table}({colname})'
            .format(index_name=index_name, table=table, colname=colname))
    conn.commit()
    #conn.close()

def read_db(sql_query, db_config):
    # For MariaDB we need to trap this error:
    # mysql.connector.errors.InterfaceError: 2003: Can't connect to MySQL server on '127.0.0.1:3306' 
    # (10055 An operation on a socket could not be performed because the system lacked sufficient buffer space or because a queue was full)
    # This post explains the problem, we're creating too many ephemeral ports (and not discarding of them properly)
    # https://blogs.msdn.microsoft.com/sql_protocols/2009/03/09/understanding-the-error-an-operation-on-a-socket-could-not-be-performed-because-the-system-lacked-sufficient-buffer-space-or-because-a-queue-was-full/
    # At the moment we do this by just adding in a wait
    db_config = _normalise_config(db_config)
    err_wait = 30.0
    
    try:
        conn = _make_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(sql_query)
    except mysql.connector.Error as err:
        if err.errno == errorcode.CR_CONN_HOST_ERROR:
            logging.warning("Caught exception '{}'. errno = '{}', waiting {} seconds and having another go".format(err, err.errno, err_wait))
            time.sleep(err_wait)
            conn = _make_connection(db_config)
            cursor = conn.cursor()
            cursor.execute(sql_query)
        else:
            raise

    colnames = [x[0] for x in cursor.description]

    while True:
        row = cursor.fetchone()
        if row is not None:
            labelled_row = OrderedDict(zip(colnames, row))
            yield labelled_row
        else:
            conn.close()
            raise StopIteration

def _normalise_config(db_config):
    """
    This is a private function which will expand a db_config string into 
    the dictionary format.
    """

    if isinstance(db_config, str):
        db_path = db_config
        db_config = db_config_template.copy()
        db_config["db_type"] = "sqlite"
        db_config["db_path"] = db_path
    return db_config

def _make_connection(db_config):
    """
    This is a private function responsible for making a connection to the database
    """

    if db_config["db_type"] == "sqlite":
        db_config["db_conn"] = sqlite3.connect(db_config["db_path"])
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        if not check_mysql_database_exists(db_config):
            create_mysql_database(db_config)

        #if db_config["db_conn"] is None:
        password = os.environ[db_config["db_pw_environ"]]
        conn = mysql.connector.connect( database=db_config["db_name"],
                                        user=db_config["db_user"], 
                                        password=password,
                                        host=db_config["db_host"],
                                        pool_name=db_config["db_name"],
                                        pool_size=32)
        db_config["db_conn"] = conn
        #else:
        #    print("Returning old connection", flush=True)
        #    conn = db_config["db_conn"]

        # Bit messy, sometimes we make a connection without db existing
        try:
            conn.database = db_config["db_name"]
        except mysql.connector.Error as err:
            if err.errno != errorcode.ER_BAD_DB_ERROR:
                raise

    return db_config["db_conn"]

def create_mysql_database(db_config):
    password = os.environ[db_config["db_pw_environ"]]
    conn = mysql.connector.connect(user=db_config["db_user"], 
                                   password=password,
                                   host=db_config["db_host"])
    cursor = conn.cursor()
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_config["db_name"]))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


def check_mysql_database_exists(db_config):
    sql_query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{}'".format(db_config["db_name"])
    password = os.environ[db_config["db_pw_environ"]]
    conn = mysql.connector.connect(user=db_config["db_user"], 
                                   password=password,
                                   host=db_config["db_host"])
    cursor = conn.cursor()
    cursor.execute(sql_query)
    #conn.commit()

    results = cursor.fetchall()
    if len(results) == 1:
        exists = True
    elif len(results) ==0:
        exists = False
    else:
        raise ValueError("Found multiple databases with the same name")

    return exists

def _create_tables_db(db_config, db_fields, tables, force):
    """
    This is a private function responsible for creating a database table
    """
    if db_config["db_type"] == "sqlite":
        table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';"
        DB_CREATE_TAIL = ")"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        table_check_query = "SELECT table_name as name FROM information_schema.tables WHERE table_schema = '{}'".format(db_config["db_name"]) + " AND table_name = '{}';"
        DB_CREATE_TAIL = ") ENGINE = MyISAM"

    conn = db_config["db_conn"]
    cursor = conn.cursor()
    for table in tables:
        DB_CREATE_ROOT = "CREATE TABLE {} (".format(table)
        
        DB_CREATE = DB_CREATE_ROOT
        for k,v in db_fields[table].items():
            if v in ["POINT", "POLYGON", "LINESTRING", "MULTIPOLYGON"]:
                logging.debug("Appending NOT NULL to {} in {} to allow spatial indexing in MariaDB/MySQL [_create_tables_db]".format(v, table))
                DB_CREATE = DB_CREATE + " ".join([k,v]) + " NOT NULL,"
            else:
                DB_CREATE = DB_CREATE + " ".join([k,v]) + ","

        DB_CREATE = DB_CREATE[0:-1] + DB_CREATE_TAIL
        if force and db_config["db_type"] == "sqlite":
            cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
            logging.warning("Force is True, so dropping table {} in database {}".format(table, db_config["db_name"]))
        elif force and (db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql"):
            cursor.execute('DROP TABLE IF EXISTS `{}`.`{}`;'.format(db_config["db_name"], table))
            #logging.warning("Drop in mysql/mariadb is not yet supported")
            logging.warning("Force is True, so dropping table {} in database {}".format(table, db_config["db_name"]))
        
        cursor.execute(table_check_query.format(table))
        result = cursor.fetchall()
        logging.debug("table_check_query result: {}".format(result))
        if len(result) != 0 and result[0][0].lower() == table.lower():
            table_exists = True
        else:
            table_exists = False

        if not table_exists:
            logging.debug("Creating table {} with statement: \n{}".format(table, DB_CREATE))    
            cursor.execute(DB_CREATE)
        else:
            logging.warning("Table {} already exists in database {}".format(table, db_config["db_name"]))

    db_config["db_conn"].commit()


