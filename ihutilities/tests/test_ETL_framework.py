#!/usr/bin/env python
# encoding, utf-8

import os
import unittest

from ihutilities.ETL_framework import (do_etl, make_point, report_input_length, 
                                       get_primary_key_from_db_fields,
                                       check_if_already_done)

from collections import OrderedDict

class TestETLFramework(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.DB_FIELDS = OrderedDict([
        ("ID"                               , "INTEGER PRIMARY KEY"),
        ("Letter"                           , "FLOAT"),
        ("Number"                           , "FLOAT"),
        ])

        cls.data_field_lookup = OrderedDict([
        ("ID"                               , "ID"),
        ("Letter"                           , "Letter"),
        ("Number"                           , "Number"),
        ])

        test_root = os.path.dirname(__file__)
        cls.datapath = os.path.join(test_root, "fixtures", "survey_csv.csv")
        cls.db_config = os.path.join(test_root, "fixtures", "do_etl.sqlite")

        db_config, status = do_etl(cls.DB_FIELDS, cls.db_config, cls.datapath, cls.data_field_lookup, mode="production", force=True)

        assert status == "Completed"

    def test_check_if_already_done(self):
        result = check_if_already_done(self.datapath, self.db_config, "d607339fd4d5ecc01e26b18d86983f305533d20c")
        self.assertEqual(result, True)

    def test_make_point(self):
        row = {"id": 1, "Easting": 123456, "Northing": 654321}
        data_field_lookup = ["Easting", "Northing"]
        point = make_point(row, data_field_lookup)
        self.assertEqual(point, "POINT(123456.0 654321.0)")

    def test_report_input_length(self):
        test_line_limit = 1000
        file_length = report_input_length(self.datapath, test_line_limit)
        self.assertEqual(file_length, 35)

    def test_get_primary_key_from_db_fields(self):
        primary_key = get_primary_key_from_db_fields(self.DB_FIELDS)
        self.assertEqual(primary_key, "ID")
