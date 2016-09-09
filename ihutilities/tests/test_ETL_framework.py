#!/usr/bin/env python
# encoding, utf-8

import os
import unittest

from ihutilities.ETL_framework import make_point, report_input_length

class TestETLFramework(unittest.TestCase):
    def test_do_etl(self):
        pass

    def test_check_if_already_done(self):
        pass

    def test_make_point(self):
        row = {"id": 1, "Easting": 123456, "Northing": 654321}
        data_field_lookup = ["Easting", "Northing"]
        point = make_point(row, data_field_lookup)
        self.assertEqual(point, "POINT(123456.0 654321.0)")

    def test_report_input_length(self):
        test_root = os.path.dirname(__file__)
        test_line_limit = 1000
        datapath = os.path.join(test_root, "fixtures", "survey_csv.csv")
        file_length = report_input_length(datapath, test_line_limit)
        self.assertEqual(file_length, 35)
