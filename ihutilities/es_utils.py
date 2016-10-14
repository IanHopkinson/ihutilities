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

try:
    es = elasticsearch.Elasticsearch(sniff_on_start=True)
except:
    logging.critical("sniff_on_start failed so Elasticsearch likely not running")

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

    #if isinstance(tables, str):
    #    tables = [tables]
    #    es_fields = {tables[0]: es_fields}
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
            Unused for Elasticsearch

    Returns:
       No return value

    Raises:

    Comments:
        
    Usage:
        >>> 
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

    return []

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

def read_es(es_query, es_config):
    es_config = _normalise_config(es_config)
    index = es_config["db_name"]

    results = es.search(index=index, body=es_query)

    for result in results["hits"]["hits"]:
        yield result["_source"]

def _normalise_config(es_config):
    """
    This is a private function which will expand a db_config string into 
    the dictionary format.
    """

    # if isinstance(es_config, str):
    #     es_path = es_config
    #     es_config = es_config_template.copy()
    #     es_config["db_type"] = "sqlite"
    #     es_config["db_path"] = es_path
    return es_config

def check_es_database_exists(es_config):
    exists = es.indices.exists(es_config["db_name"])
    return exists

def _create_tables_es(es_config, es_fields, tables, force):
    """
    This is a private function responsible for creating a database table
    """

    status = es.indices.create(index=es_config["db_name"], ignore=400)
    logger.info("Created index '{}' with status {}".format(es_config["db_name"], status))
    status = es.indices.put_mapping(index=es_config["db_name"], ignore=400, doc_type=tables, body=es_fields["mappings"])
    logger.info("Put mapping '{}' on '{}' with status {}".format(es_fields["mappings"], tables, status))

    return status
