#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to Elasticsearch stores, it is based very directly
on the db_utils functions in ihutilities
"""

import elasticsearch
import logging
import socket
import urllib3

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

# This is problematic because we throw a bunch of ugly looking errors when we try to do this, and its slow


    
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(('localhost',9200))
if result == 0:
   es = elasticsearch.Elasticsearch(sniff_on_start=True)

def is_elasticsearch_running():
    answer = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost',9200))
    if result == 0:
        answer = True
    else:
        answer = False

    return answer

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

def update_to_es(data, es_config, es_fields, table="data", key="UPRN"):
    """
    This function updates rows in an elasticsearch database

    Args:
       data (list of dictionaries):
            List of dictionaries of data to update to database, order matches db_fields
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
        >>> 
    """
    es_config = _normalise_config(es_config)

    # We need to find the _id of the documents that match
    ids_query = {
      "query": {
        "constant_score": {
          "filter": {
            "term": {
              "{}".format(key): 3
            }
          }
        }
      }
    }
    # We can set 
    for row in data:
        ids_query = {
                    "query": {
                        "constant_score": {
                            "filter": {
                                "term": {
                                    "{}".format(key): row[key]
                                    }
                                }
                            }
                        }
                    }

        row.pop(key)
        data_modified = dict(row)
        results = es.search(index=es_config["db_name"], body=ids_query)
        for matching_record in results["hits"]["hits"]:
            _id = matching_record["_id"]
            es.update(index=es_config["db_name"],doc_type=table,id=_id,
                        body={"doc": data_modified})
    
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
    for table in tables:
        status = es.indices.put_mapping(index=es_config["db_name"], ignore=400, doc_type=table, body=es_fields[table]["mappings"])
        logger.info("Put mapping '{}' on '{}' with status {}".format(es_fields[table]["mappings"], table, status))

    return status
