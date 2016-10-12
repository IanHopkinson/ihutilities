#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to Elasticsearch stores, it is based very directly
on the db_utils functions in ihutilities
"""

import elasticsearch
import logging

from elasticsearch import helpers

# This is compatible with the db_utils config template
es_config_template = {"db_name": "test",
             "db_user": "root",
             "db_pw_environ": "MARIA_DB_PASSWORD",
             "db_host": "127.0.0.1",
             "db_conn": None,
             "db_type": "elasticsearch",
             "db_path": None
            }

logger = logging.getLogger(__name__)


# Throught this we will use the equivalence:
# Index in Elasticsearch = database in Mysql/Mariadb
# Type in Elasticsearch = table in Mysql/Mariadb
# Documents in Elasticsearch = rows in Mysql/Mariadb
# Fields in Elasticsearch = columns in Mysql/Mariadb
#

es = elasticsearch.Elasticsearch()

def configure_es(es_config, es_fields, tables="data", force=False):
    """This function sets up an Elasticsearch database

    Args:
       es_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       es_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Kwargs:
       tables (string or list of strings): 
            names of tables required, keys to db_fields
       force (bool): 
            If using sqlite, force=True deletes existing files of db_config 

    Returns:
       es_config structure

    Raises:

    Usage:
        >>> 
    """
    # Cunning polymorphism: 
    # If we get a list and string then we convert them to a dictionary and a list
    # for backward compatibility

    if isinstance(tables, str):
        tables = [tables]
        es_fields = {tables[0]: es_fields}
    # Convert old db_path string to db_config dictionary
    es_config = _normalise_config(es_config)

    # Delete database if force is true
    if es_config["db_type"] == "elasticsearch":
        if force:
            status = delete_es(es_config)
    # Create tables, as specified
    _create_tables_es(es_config, es_fields, tables, force)

    return es_config

def delete_es(es_config):
    """This function deletes a named elasticsearch index 

    Args:
       es_config (str or dict): 
            db_name contains the name of the index to be deleted

    Kwargs:
       tables (string or list of strings): 
            names of tables required, keys to db_fields
       force (bool): 
            If using sqlite, force=True deletes existing files of db_config 

    Returns:
       es_config structure

    Raises:

    Usage:
        >>> 
    """
    status = es.indices.delete(index=es_config["db_name"], ignore=[400, 404])
    logger.info("Index '{}' deleted with status = {}".format(es_config["db_name"], status))

    return status

def write_to_es(data, es_config, es_fields, table="data", whatever=False):
    """
    This function writes a list of rows to an elasticsearch database

    Args:
       data (list of dictionaries):
            List of dictionaries - this differs from write_to_db which takes a list of lists
       db_config (str or dict): 
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Kwargs:
       table (str): 
            name of table to which we are writing, key to db_fields
       whatever (bool):
            If true each item is tried individually and only those accepted are written, list of those 
            not inserted is returned

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
    es_config = _normalise_config(es_config)

    actions = []
    for row in data:
        action = {
                "_index": es_config["db_name"],
                "_type": table,
                #"_id": line_count,
                "_source": row
            }

        actions.append(action)
        
    helpers.bulk(es, actions) 

    return

# def update_to_db(data, db_config, db_fields, table="property_data", key="UPRN"):
#     """
#     This function updates rows in a sqlite or MariaDB/MySQL database

#     Args:
#        data (list of lists):
#             List of lists of data to update to database, order matches db_fields
#        db_config (str or dict): 
#             For sqlite a file path in a string is sufficient, MariaDB/MySQL require
#             a dictionary and example of which is found in db_config_template
#        db_fields (OrderedDict):
#             A list of fieldnames in an OrderedDict containing the fields to update and
#             the key field in the order in which the fields are presented in the data lists

#     Kwargs:
#        table (str): 
#             name of table to which we are writing, key to db_fields
#        key (str):
#             the field which forms the key of the update

#     Returns:
#        No return value

#     Raises:

#     Usage:
#         >>> db_fields = OrderedDict([
#               ("UPRN","INTEGER PRIMARY KEY"),
#               ("PropertyID", "INT"),
#               ("Addr1", "TEXT"),                   
#         ])
#         >>> db_filename = "test_write_db.sqlite"
#         >>> db_dir = "ihutilities\\fixtures"
#         >>> db_file_path = os.path.join(db_dir, db_filename)
#         >>> data = [(1, 2, "hello"),
#                     (2, 3, "Fred"),
#                     (3, 3, "Beans")]
#         >>> update_fields = ["Addr1", "UPRN"]
#         >>> update = [("Some", 3)] 
#         >>> update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")
#     """
#     db_config = _normalise_config(db_config)
#     if db_config["db_type"] == "sqlite":
#         DB_UPDATE_TAIL = " WHERE {} = ?".format(key)
#         PLACEHOLDER = " = ?,"
#     elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
#         DB_UPDATE_TAIL = " WHERE {} = %s".format(key)
#         PLACEHOLDER = " = %s,"

#     conn = _make_connection(db_config)
#     cursor = conn.cursor()
#     DB_UPDATE_ROOT = "UPDATE {} SET ".format(table)
    
#     key_index = db_fields.index(key)

    
#     for row in data:
#         # print(update_statement, [x for x in row])
#         key_val = row[key_index]
#         update_fields = []
#         update_data = []
#         for i, _ in enumerate(row):
#             if i != key_index and row[i] is not None:
#                 update_fields.append(db_fields[i])
#                 update_data.append(row[i])
                
#         DB_FIELDS = DB_UPDATE_ROOT
#         for k in update_fields:
#             DB_FIELDS = DB_FIELDS + k + PLACEHOLDER 
#             update_statement = DB_FIELDS[0:-1] + DB_UPDATE_TAIL

#         update_data.append(key_val)
#         if len(update_fields) != 0:
#             cursor.execute(update_statement, update_data)

#     conn.commit()
#     conn.close()

# def drop_db_tables(file_path, tables):
#     conn = sqlite3.connect(file_path)
#     for table in tables:
#         conn.execute('DROP TABLE IF EXISTS {}'.format(table))
#     conn.close()

# def finalise_db(db_config, index_name="idx_postcode", table="property_data", colname="postcode", spatial=False):
#     """
#     This function creates an index in a sqlite or MariaDB/MySQL database

#     Args:
#        db_config (str or dict): 
#             For sqlite a file path in a string is sufficient, MariaDB/MySQL require
#             a dictionary and example of which is found in db_config_template

#     Kwargs:
#        index_name (str): 
#             name of the index to be created
#        table (str):
#             the table on which the index is to be created
#        colname (str):
#             the column on which the index is to be created
#        spatial (bool):
#             True for a spatial index, false otherwise

#     Returns:
#        No return value

#     Raises:

#     Usage:
#     """

#     db_config = _normalise_config(db_config)

#     conn = _make_connection(db_config)
#     cursor = conn.cursor()

#     if isinstance(colname, list):
#         colname = ",".join(colname)
    
#     logger.info("Creating index named '{}' on column(s) '{}'".format(index_name, colname))
#     if spatial:
#         cursor.execute('CREATE SPATIAL INDEX {index_name} on {table}({colname})'
#             .format(index_name=index_name, table=table, colname=colname))
#     else:
#         cursor.execute('CREATE INDEX {index_name} on {table}({colname} ASC)'
#             .format(index_name=index_name, table=table, colname=colname))
#     conn.commit()
#     conn.close()

# def read_db(sql_query, db_config):
#     # For MariaDB we need to trap this error:
#     # mysql.connector.errors.InterfaceError: 2003: Can't connect to MySQL server on '127.0.0.1:3306' 
#     # (10055 An operation on a socket could not be performed because the system lacked sufficient buffer space or because a queue was full)
#     # This post explains the problem, we're creating too many ephemeral ports (and not discarding of them properly)
#     # https://blogs.msdn.microsoft.com/sql_protocols/2009/03/09/understanding-the-error-an-operation-on-a-socket-could-not-be-performed-because-the-system-lacked-sufficient-buffer-space-or-because-a-queue-was-full/
#     # At the moment we do this by just adding in a wait
#     db_config = _normalise_config(db_config)
#     err_wait = 30.0
    
#     try:
#         conn = _make_connection(db_config)
#         cursor = conn.cursor()
#         cursor.execute(sql_query)
#     except mysql.connector.Error as err:
#         if err.errno == errorcode.CR_CONN_HOST_ERROR:
#             logger.warning("Caught exception '{}'. errno = '{}', waiting {} seconds and having another go".format(err, err.errno, err_wait))
#             time.sleep(err_wait)
#             conn = _make_connection(db_config)
#             cursor = conn.cursor()
#             cursor.execute(sql_query)
#         else:
#             raise

#     colnames = [x[0] for x in cursor.description]

#     #rows = cursor.fetchall()
#     #conn.close()

#     # for row in rows:
#     #     if row is not None:
#     #         labelled_row = OrderedDict(zip(colnames, row))
#     #         yield labelled_row

#     while True:
#        row = cursor.fetchone()
#        if row is not None:
#            labelled_row = OrderedDict(zip(colnames, row))
#            yield labelled_row
#        else:
#            conn.close()
#            raise StopIteration

def _normalise_config(es_config):
    """
    This is a private function which will expand a db_config string into 
    the dictionary format.
    """

    if isinstance(es_config, str):
        es_path = es_config
        es_config = es_config_template.copy()
        es_config["db_type"] = "sqlite"
        es_config["db_path"] = es_path
    return es_config

# def _make_connection(db_config):
#     """
#     This is a private function responsible for making a connection to the database
#     """

#     if db_config["db_type"] == "sqlite":
#         db_config["db_conn"] = sqlite3.connect(db_config["db_path"])
#     elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
#         if not check_mysql_database_exists(db_config):
#             create_mysql_database(db_config)

#         # This code much fiddled with, essentially I was trying to do my own connection pooling
#         # on top of the connectors pooling and it didn't work.
#         # I was getting pool exhaustion because I wasn't closing connections, this should now be fixed (fingers crossed)
#         #if db_config["db_conn"] is None or True:
#         password = os.environ[db_config["db_pw_environ"]]
#         conn = mysql.connector.connect( database=db_config["db_name"],
#                                         user=db_config["db_user"], 
#                                         password=password,
#                                         host=db_config["db_host"],
#                                         pool_name=db_config["db_name"],
#                                         pool_size=10)
#         db_config["db_conn"] = conn
#         #else:
#         #    print("Returning old connection", flush=True)
#         #    conn = db_config["db_conn"]

#         # Bit messy, sometimes we make a connection without db existing
#         try:
#             conn.database = db_config["db_name"]
#         except mysql.connector.Error as err:
#             if err.errno != errorcode.ER_BAD_DB_ERROR:
#                 raise

#     return db_config["db_conn"]


def check_es_database_exists(es_config):
    exists = es.indices.exists(es_config["db_name"])
    return exists

def _create_tables_es(es_config, es_fields, tables, force):
    """
    This is a private function responsible for creating a database table
    """

    status = es.indices.create(index=es_config["db_name"], ignore=400, body=es_fields)
    logger.info("Created database '{}' with status {}".format(es_config["db_name"], status))

    return status
