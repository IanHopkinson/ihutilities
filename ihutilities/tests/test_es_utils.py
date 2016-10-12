#!/usr/bin/env python
# encoding: utf-8

import unittest
import logging
import os
import time

from ihutilities.es_utils import (es_config_template, configure_es, delete_es,
                                    check_es_database_exists) 

                                # write_to_es,
                                #   _make_connection, read_es,
                                #   update_to_es, finalise_es,
                                #   check_mysql_database_exists)

logging.basicConfig(level=logging.INFO)

class ElasticsearchUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # cls.es_fields = OrderedDict([
        #       ("UPRN","INTEGER PRIMARY KEY"),
        #       ("PropertyID", "INT"),
        #       ("Addr1", "TEXT"),                   
        # ])

        cls.es_fields = {
                "settings": {
                    #"number_of_shards": 1,
                    #"number_of_replicas": 0
                },
                "mappings": {
                    "datarecord": { # 
                        "properties": {
                            "UPRN": {
                                "type": "string"
                            },
                            "PropertyID": {
                                "type": "string"
                            },
                            "Addr1": {
                                "type": "string"
                            }
                        }
                    }
                 }
            }

        test_root = os.path.dirname(__file__)
        cls.db_dir = os.path.join(test_root, "fixtures")

        #if os.path.isfile(cls.db_file_path):
        #    os.remove(cls.db_file_path)

    def test_configure_es(self):
        # Connect to engine and delete test table if it exists
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        status = configure_es(es_config, self.es_fields, tables="test", force=True)

        # Test es exists
        exists = check_es_database_exists(es_config)
        self.assertEqual(exists, True)

        # Check details of config


    # def test_write_to_db(self):
    #     db_filename = "test_write_db.sqlite"
    #     db_file_path = os.path.join(self.db_dir, db_filename)
    #     if os.path.isfile(db_file_path):
    #         os.remove(db_file_path)
    #     data = [(1, 2, "hello"),
    #             (2, 3, "Fred"),
    #             (3, 3, "Beans")]
    #     configure_db(db_file_path, self.db_fields, tables="test")
    #     write_to_db(data, db_file_path, self.db_fields, table="test")
    #     with sqlite3.connect(db_file_path) as c:
    #         cursor = c.cursor()
    #         cursor.execute("""
    #             select * from test;
    #         """)
    #         rows = cursor.fetchall()
    #         assert_equal(data, rows)

    # def test_update_to_db(self):
    #     db_filename = "test_update_db.sqlite"
    #     db_file_path = os.path.join(self.db_dir, db_filename)
    #     if os.path.isfile(db_file_path):
    #         os.remove(db_file_path)
    #     data = [(1, 2, "hello"),
    #             (2, 3, "Fred"),
    #             (3, 3, "Beans")]
    #     configure_db(db_file_path, self.db_fields, tables="test", force=True)
    #     write_to_db(data, db_file_path, self.db_fields, table="test")

    #     update_fields = ["Addr1", "UPRN"]
    #     update = [("Some", 3)] 
    #     update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")

    #     with sqlite3.connect(db_file_path) as c:
    #         cursor = c.cursor()
    #         cursor.execute("""
    #             select Addr1 from test where UPRN = 3 ;
    #         """)
    #         rows = cursor.fetchall()
    #         expected = ("Some", )
    #         assert_equal(expected, rows[0])

    # def test_update_mariadb(self):
    #     db_config = db_config_template.copy()
    #     db_config = configure_db(db_config, self.db_fields, tables="test", force=True)
    #     data = [(1, 2, "hello"),
    #             (2, 3, "Fred"),
    #             (3, 3, "Beans")]
    #     write_to_db(data, db_config, self.db_fields, table="test")

    #     update_fields = ["Addr1", "UPRN"]
    #     update = [("Some", 3)] 
    #     update_to_db(update, db_config, update_fields, table="test", key="UPRN")

    #     conn = _make_connection(db_config)
    #     cursor = conn.cursor()
    #     cursor.execute("""
    #         select Addr1 from test where UPRN = 3 ;
    #     """)
    #     rows = cursor.fetchall()
    #     expected = ("Some", )
    #     assert_equal(expected, rows[0])

    # def test_read_es(self):
    #     db_filename = "test_finalise_db.sqlite"
    #     db_config = os.path.join(self.db_dir, db_filename)
    #     if os.path.isfile(db_config):
    #         os.remove(db_config)
    #     data = [(1, 2, "hello"),
    #             (2, 3, "Fred"),
    #             (3, 3, "Beans")]
    #     configure_db(db_config, self.db_fields, tables="test")
    #     write_to_db(data, db_config, self.db_fields, table="test")

    #     sql_query = "select * from test;"

    #     for i, row in enumerate(read_db(sql_query, db_config)):
    #         test_data = OrderedDict(zip(self.db_fields.keys(), data[i]))
    #         assert_equal(row, test_data)

    # def test_check_database_exists(self):
    #     db_config = db_config_template.copy()
    #     db_config["db_name"] = "djnfsjnf"

    #     assert_equal(check_mysql_database_exists(db_config), False)

    #     db_config["db_name"] = "INFORMATION_SCHEMA"
    #     assert_equal(check_mysql_database_exists(db_config), True)
