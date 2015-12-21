#!/usr/bin/env python
# encoding: utf-8

import unittest
import os
import sqlite3

from collections import OrderedDict
from nose.tools import assert_equal

from db_utils import configure_db, write_to_db, update_to_db, finalise_db

class DatabaseUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),                   
        ])
        cls.db_dir = "db\\fixtures"
        
        #if os.path.isfile(cls.db_file_path):
        #    os.remove(cls.db_file_path)


    def test_configure_db(self):
        db_filename = "test_config_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        configure_db(db_file_path, self.db_fields, tables="test")
        # Test file exists
        assert_equal(True, os.path.isfile(db_file_path)) 
        # Do a schema query
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute("""
                select * from test;
            """)
            exp_columns = set([x for x in self.db_fields.keys()]) 
            obs_columns = set([x[0] for x in cursor.description])
            assert_equal(exp_columns, obs_columns)


    def test_configure_multi_db(self):
        db_filename = "test_config_multi_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        tables = ["test1", "test2"]
        db_fields2 = OrderedDict([
              ("UPRN2","INTEGER PRIMARY KEY"),
              ("PropertyID2", "INT"),
              ("Addr2", "TEXT"),                   
        ])
        db_field_set = {"test1": self.db_fields,"test2": db_fields2}
        configure_db(db_file_path, db_field_set, tables=tables)
        # Test file exists
        assert_equal(True, os.path.isfile(db_file_path)) 
        # Do a schema query
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            for i in range(0,2):
                cursor.execute("select * from {};".format(tables[i]))
                exp_columns = set([x for x in db_field_set[tables[i]].keys()]) 
                obs_columns = set([x[0] for x in cursor.description])
                assert_equal(exp_columns, obs_columns)

    def test_write_to_db(self):
        db_filename = "test_write_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"),
                (2, 3, "Fred"),
                (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields)
        write_to_db(data, db_file_path, self.db_fields)
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute("""
                select * from property_data;
            """)
            rows = cursor.fetchall()
            assert_equal(data, rows)

    def test_update_to_db(self):
        db_filename = "test_update_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"),
                (2, 3, "Fred"),
                (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")

        update_fields = ["Addr1", "UPRN"]
        update = [("Some", 3)] 
        update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute("""
                select Addr1 from test where UPRN = 3 ;
            """)
            rows = cursor.fetchall()
            expected = ("Some", )
            assert_equal(expected, rows[0])

    def test_update_to_db_no_nones(self):
        db_filename = "test_update_db2.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"),
                (2, 3, "Fred"),
                (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")

        update_fields = ["Addr1", "PropertyID", "UPRN"]
        update = [("Some", 1, 3), (None, 1, 2)] 
        update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute("""
                select * from test;
            """)
            rows = cursor.fetchall()
            assert_equal(1, rows[1][1])
            assert_equal(1, rows[2][1])
            assert_equal("Fred", rows[1][2])

    def test_finalise_db(self):
        db_filename = "test_finalise_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"),
                (2, 3, "Fred"),
                (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")
        finalise_db(db_file_path, index_name="idx_addr1", table="test", colname="Addr1")