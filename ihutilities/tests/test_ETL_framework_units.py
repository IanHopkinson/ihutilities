#!/usr/bin/env python
# encoding, utf-8

import os
import unittest

from collections import OrderedDict

from ihutilities.ETL_framework import (make_point, report_input_length,
                                       get_primary_key_from_db_fields,
                                       get_source_generator,
                                       make_row)



class TestETLUnits(unittest.TestCase):
    """
    Class containing tests for the units of do_etl
    """
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

        cls.test_root = os.path.dirname(__file__)
        cls.datapath = os.path.join(cls.test_root, "fixtures", "survey_csv.csv")
        cls.db_config = os.path.join(cls.test_root, "fixtures", "do_etl.sqlite")

        if os.path.isfile(cls.db_config):
            os.remove(cls.db_config)

    def test_make_point(self):
        row = {"id": 1, "Easting": 123456, "Northing": 654321}
        data_field_lookup = ["Easting", "Northing"]
        point = make_point(row, data_field_lookup)
        self.assertEqual(point, "POINT(123456.0 654321.0)")

    def test_report_input_length(self):
        test_line_limit = 1000
        file_length = report_input_length(get_source_generator, test_line_limit, self.datapath, False, ",", "utf-8-sig")
        self.assertEqual(file_length, 35)

    def test_get_primary_key_from_db_fields(self):
        primary_key = get_primary_key_from_db_fields(self.DB_FIELDS)
        self.assertEqual(primary_key, "ID")

    def test_make_row_primary_key_to_null_for_autoinc(self):
        # Really we should put the make_row tests in a separate class with their own setup
        input_row = {"Letter": "A", "Number": 1}
        data_path = ""
        autoinc_lookup = self.data_field_lookup.copy()
        autoinc_lookup["ID"] = None
        db_fields = self.DB_FIELDS.copy()
        null_equivalents = []
        autoinc = True
        primary_key = "ID"

        data_row = make_row(input_row, data_path, autoinc_lookup, db_fields, null_equivalents, autoinc, primary_key)

        self.assertEqual(data_row, OrderedDict([('ID', None), ('Letter', 'A'), ('Number', 1)]))

    def test_make_row_raises_an_error_if_field_missing(self):
        input_row = {"Goblin": "A", "Number": 1}
        data_path = ""
        autoinc_lookup = self.data_field_lookup.copy()
        autoinc_lookup["ID"] = None
        db_fields = self.DB_FIELDS.copy()
        null_equivalents = []
        autoinc = True
        primary_key = "ID"

        self.assertRaises(KeyError, make_row, input_row, data_path, autoinc_lookup, db_fields, null_equivalents, autoinc, primary_key)
