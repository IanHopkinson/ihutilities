#!/usr/bin/env python
# encoding: utf-8

import os
import sqlite3
import mysql.connector
from mysql.connector import errorcode


def configure_db(db_path, db_fields, tables="property_data", force=False):
    """
    We build a database using an ordered dict of field: type entries and a file_path
    this assumes a sqlite3 database which is removed if it already exists
    """
    # Cunning polymorphism: 
    # If we get a list and string then we convert them to a dictionary and a list
    # for backward compatibility
    if isinstance(tables, str):
        tables = [tables]
        db_fields = {tables[0]: db_fields}

        #any(isinstance(el, list) for el in input_list)

    if os.path.isfile(db_path) and force:
        os.remove(db_path)

    conn = sqlite3.connect(db_path)

    for table in tables:
        DB_CREATE_ROOT = "CREATE TABLE {} (".format(table)
        DB_CREATE_TAIL = ")"
        DB_CREATE = DB_CREATE_ROOT
        for k,v in db_fields[table].items():
            DB_CREATE = DB_CREATE + " ".join([k,v]) + ","

        DB_CREATE = DB_CREATE[0:-1] + DB_CREATE_TAIL
        if force:
            conn.execute('DROP TABLE IF EXISTS {}'.format(table))
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format(table))
        result = cursor.fetchall()
        if len(result) != 0 and result[0][0] == table:
            table_exists = True
        else:
            table_exists = False

        if not table_exists:    
            cursor.execute(DB_CREATE) 

    conn.commit()
    conn.close()

def create_tables_db(db_path, db_fields, table):
    pass

def write_to_db(data, file_path, db_fields, table="property_data"):
    """
    we write data into the property_data table from an array of dictionaries of
    field: value entries 
    """
    conn = sqlite3.connect(file_path)

    DB_INSERT_ROOT = "INSERT INTO {} (".format(table)
    DB_INSERT_MIDDLE = ") VALUES ("
    DB_INSERT_TAIL = ")"

    DB_FIELDS = DB_INSERT_ROOT
    DB_PLACEHOLDERS = DB_INSERT_MIDDLE

    for k in db_fields.keys():
        DB_FIELDS = DB_FIELDS + k + ","
        DB_PLACEHOLDERS = DB_PLACEHOLDERS + "?,"


    INSERT_statement = DB_FIELDS[0:-1] + DB_PLACEHOLDERS[0:-1] + DB_INSERT_TAIL

    conn.executemany(INSERT_statement, data)

    conn.commit()
    conn.close()

def update_to_db(data, file_path, db_fields, table="property_data", key="UPRN"):
    """
    We *update* data into the property_data table from an array of dictionaries of
    field: value entries

    db_fields is a list of field names, the last in the list is the join keys
    data is a list of lists of the elements to be inserted 
    """
    conn = sqlite3.connect(file_path)

    DB_UPDATE_ROOT = "UPDATE {} SET ".format(table)
    DB_UPDATE_TAIL = " WHERE {} = ?".format(key)

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
            DB_FIELDS = DB_FIELDS + k + " = ?,"
            update_statement = DB_FIELDS[0:-1] + DB_UPDATE_TAIL

        update_data.append(key_val)
        if len(update_fields) != 0:
            conn.execute(update_statement, update_data)

    conn.commit()
    conn.close()

def drop_db_tables(file_path, tables):
    conn = sqlite3.connect(file_path)
    for table in tables:
        conn.execute('DROP TABLE IF EXISTS {}'.format(table))
    conn.close()

def finalise_db(file_path, index_name="idx_postcode", table="property_data", colname="postcode" ):
    conn = sqlite3.connect(file_path)
    conn.execute('CREATE INDEX {index_name} on {table}({colname})'
        .format(index_name=index_name, table=table, colname=colname))
    conn.commit()
    conn.close()

# These functions create a mariadb database, ultimately we want to merge them with the
# sqlite routines above 
DB_NAME = "property_data"

def configure_mariadb(db_fields, tables="table1", force=False):
    password = os.environ['MARIA_DB_PASSWORD']
    cnx = mysql.connector.connect(user='root', password=password,
                                 host='127.0.0.1')
    cursor = cnx.cursor()

    # This creates the database if it doesn't exist
    try:
        cnx.database = DB_NAME    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database_mariadb(cursor)
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)

    # This creates the appropriate table in the database
    create_table_mariadb(cursor, db_fields, table=tables)

def create_database_mariadb(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def create_table_mariadb(cursor, db_fields, table="listed_buildings"):
    """
    We build a database using an ordered dict of field: type entries and a file_path
    this assumes a sqlite3 database which is removed if it already exists
    """

    DB_CREATE_ROOT = "CREATE TABLE {} (".format(table)
    DB_CREATE_TAIL = ") ENGINE = MyISAM"
    DB_CREATE = DB_CREATE_ROOT
    for k,v in db_fields.items():
        DB_CREATE = DB_CREATE + " ".join([k,v]) + ","

    DB_CREATE = DB_CREATE[0:-1] + DB_CREATE_TAIL

    cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
    cursor.execute(DB_CREATE) 


def write_to_mariadb(data, db_fields, table="property_data"):
    """
    we write data into the property_data table from an array of dictionaries of
    field: value entries 
    """
    password = os.environ['MARIA_DB_PASSWORD']
    cnx = mysql.connector.connect(user='root', password=password,
                                 host='127.0.0.1',
                                 database='property_data')
    cursor = cnx.cursor()

    DB_INSERT_ROOT = "INSERT INTO {} (".format(table)
    DB_INSERT_MIDDLE = ") VALUES ("
    DB_INSERT_TAIL = ")"

    DB_FIELDS = DB_INSERT_ROOT
    DB_PLACEHOLDERS = DB_INSERT_MIDDLE

    for k in db_fields.keys():
        DB_FIELDS = DB_FIELDS + k + ","
        DB_PLACEHOLDERS = DB_PLACEHOLDERS + "{},"

    for row in data:
        tmp = [v for k, v in row.items()]
        INSERT_statement = (DB_FIELDS[0:-1] + DB_PLACEHOLDERS[0:-1] + DB_INSERT_TAIL).format(*tmp)
        #tmp = [v for k,v in row.items()]
        #print(tmp)
        cursor.execute(INSERT_statement)

    cnx.commit()